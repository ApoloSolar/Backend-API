# -*- coding: utf-8 -*-
"""
============================================================
  API v2 — APOLO SOLAR  (Railway / PostgreSQL)
============================================================
API que le o banco PostgreSQL (schema v2) e serve os dados
para o dashboard, em JSON.

NOVIDADES DA v2:
  1. SCHEMA v2 — le as tabelas novas: 'leitura_mppt' e
     'leitura_string' (a antiga 'leitura_canal' nao existe
     mais).
  2. CORRECAO DE MEMORIA — a API v1 carregava dezenas de
     milhares de linhas na memoria (fetchall de um dia
     inteiro de canais), o que esgotava a RAM no Railway.
     A v2:
       - resume os dados NO BANCO (SUM, MAX, agrupamentos)
         e devolve so o resultado, nao as linhas cruas;
       - o endereco de canais devolve apenas a leitura MAIS
         RECENTE de cada inversor (e o que os cards do
         dashboard usam), nao o dia inteiro;
       - reaproveita UMA conexao com o banco, em vez de
         abrir uma nova a cada chamada.

NOVIDADES DESTA REVISAO (reducao de egress):
  3. GZIP — comprime as respostas JSON (~80% menores). O
     navegador descomprime sozinho; nenhum endpoint muda.
  4. CACHE — adiciona Cache-Control: periodo ja encerrado
     (dia/mes/ano passado) fica cacheado por 24h; periodo
     atual, por 60s (o coletor so grava de 5 em 5 min). O
     navegador para de rebaixar dados que nao mudaram.
  5. RATE LIMIT — limita requisicoes por IP (corta picos de
     bots/scanners sem afetar o dashboard).

NOVIDADES DESTA REVISAO (multi-usina):
  6. FILTRO POR USINA — as rotas do modo diario (/dia,
     /dia/{data}/canais e /dia/{data}/curva-inversores)
     aceitam o parametro de query ?usina=<slug> (default
     "pk") e filtram as leituras por usina.slug. Sem isso a
     API somava TODAS as usinas juntas (PK + Ibiracu), o que
     contaminava os graficos de PK e fazia Ibiracu aparecer
     com os dados de PK.

CREDENCIAIS: DATABASE_URL vem de variavel de ambiente.

ENDERECOS:
  /                      -> pagina inicial com os links
  /saude                 -> a API e o banco estao no ar?
  /resumo                -> visao geral: total, periodo, status
  /inversores            -> inversores e contagem de leituras
  /ultimas               -> 20 leituras mais recentes
  /dia/{data}            -> resumo + curva do dia (AAAA-MM-DD)
                            ?usina=<slug> (default "pk")
  /dia/{data}/canais     -> canais da leitura mais recente
                            de cada inversor naquele dia
                            ?usina=<slug> (default "pk")
  /dia/{data}/curva-inversores -> serie de pac_kw por inversor
                            ?usina=<slug> (default "pk")
  /mensal/{aaaa-mm}      -> resumo do mes (le resumo_dia)
                            ?usina=<slug> (default "pk")
  /anual/{aaaa}          -> resumo do ano (agrega resumo_dia)
                            ?usina=<slug> (default "pk")
  /checagem              -> procura problemas nos dados
============================================================
"""

import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.datastructures import MutableHeaders

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


# ============================================================
# CONFIGURACAO
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Variavel DATABASE_URL nao configurada.")

# O Railway roda em UTC. O Brasil (Espirito Santo) e UTC-3.
FUSO_BRASIL = timezone(timedelta(hours=-3))

app = FastAPI(title="API Apolo Solar v2")

# CORS — permite que o dashboard (noutro endereco) chame a API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# OTIMIZACOES DE EGRESS  (transparente — nao altera endpoints)
# ============================================================
# Reduz o trafego de rede sem mudar nenhuma resposta:
#   - cache: o navegador para de rebaixar dados que nao mudaram
#   - gzip: respostas JSON ~80% menores
#   - rate limit: corta picos de bots/scanners por IP
# A ordem de aplicacao deixa o rate limit por fora (rejeita bot
# antes de gastar processamento), o gzip no meio e o cache por
# dentro (so carimba o cabecalho).

# Endpoints de "estado atual": cache curto (60s).
_DINAMICOS = {"/ultimas", "/resumo", "/inversores", "/saude"}
_CACHE_LONGO = "public, max-age=86400"   # 24h — periodo ja encerrado
_CACHE_CURTO = "public, max-age=60"      # 60s — periodo atual / estado


def _cache_para_caminho(caminho, agora):
    """Decide o Cache-Control a partir do caminho da requisicao.
    Compara a data/mes/ano pedido com o atual: periodo encerrado
    recebe cache longo; periodo em andamento, cache curto.
    Devolve a string de Cache-Control ou None (sem cache)."""
    partes = caminho.strip("/").split("/")
    if not partes or not partes[0]:
        return None
    try:
        # /dia/AAAA-MM-DD   (e tambem .../canais, .../curva-inversores)
        if partes[0] == "dia" and len(partes) >= 2:
            d = datetime.strptime(partes[1], "%Y-%m-%d").date()
            return _CACHE_LONGO if d < agora.date() else _CACHE_CURTO
        # /mensal/AAAA-MM
        if partes[0] == "mensal" and len(partes) >= 2:
            ano, mes = partes[1].split("-")
            if (int(ano), int(mes)) < (agora.year, agora.month):
                return _CACHE_LONGO
            return _CACHE_CURTO
        # /anual/AAAA
        if partes[0] == "anual" and len(partes) >= 2:
            return _CACHE_LONGO if int(partes[1]) < agora.year else _CACHE_CURTO
    except (ValueError, IndexError):
        return None  # formato inesperado: nao mexe no cabecalho
    if caminho in _DINAMICOS:
        return _CACHE_CURTO
    return None


class CacheHeaderMiddleware:
    """Middleware ASGI puro: apenas ADICIONA o cabecalho Cache-Control
    nas respostas GET 200. Nunca le nem altera o corpo da resposta,
    entao convive sem problemas com o GZip e o rate limit."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or scope.get("method") != "GET":
            await self.app(scope, receive, send)
            return
        cc = _cache_para_caminho(scope.get("path", ""),
                                 datetime.now(FUSO_BRASIL))
        if cc is None:
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message):
            if (message["type"] == "http.response.start"
                    and message.get("status") == 200):
                headers = MutableHeaders(raw=message["headers"])
                headers["Cache-Control"] = cc
            await send(message)

        await self.app(scope, receive, send_wrapper)


def _ip_cliente(request):
    """IP real do cliente. Atras do proxy do Railway o IP verdadeiro
    vem no cabecalho X-Forwarded-For; o request.client seria o proxy."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return get_remote_address(request)


# 1) Cache (so adiciona cabecalho — ASGI puro)
app.add_middleware(CacheHeaderMiddleware)

# 2) GZip — comprime respostas a partir de 500 bytes
app.add_middleware(GZipMiddleware, minimum_size=500)

# 3) Rate limit por IP (300/min e folgado para o dashboard,
#    apertado para bots). Ajuste se telas legitimas tomarem 429.
limiter = Limiter(key_func=_ip_cliente, default_limits=["300/minute"])
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)


@app.exception_handler(RateLimitExceeded)
def _limite_excedido(request, exc):
    return JSONResponse(
        status_code=429,
        content={"detail": "Muitas requisicoes. Tente novamente em instantes."},
    )


# ============================================================
# ACESSO AO BANCO — POOL DE CONEXOES
# ============================================================
# Em vez de abrir uma conexao nova a cada requisicao (custoso
# em memoria), mantemos um pequeno pool de conexoes reutilizadas.
# min_size=1, max_size=3 e suficiente para o dashboard e mantem
# o uso de memoria baixo.

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=3,
    kwargs={"connect_timeout": 15},
    open=True,
)


def consultar(sql, params=()):
    """Executa um SELECT e devolve as linhas como lista de dicionarios.
    Usa uma conexao do pool (reaproveitada)."""
    try:
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, params)
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"Erro ao consultar o banco: {e}")


def um(sql, params=()):
    """Atalho: executa um SELECT e devolve apenas a primeira linha."""
    linhas = consultar(sql, params)
    return linhas[0] if linhas else None


def fmt(dt):
    """Formata um datetime do banco para texto 'AAAA-MM-DD HH:MM'.
    Os horarios sao gravados em UTC; convertemos para o Brasil."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(FUSO_BRASIL).strftime("%Y-%m-%d %H:%M")


def faixa_do_dia(data):
    """Valida a data e devolve (inicio, fim) como datetimes no
    fuso do Brasil — o intervalo [inicio, fim) do dia pedido."""
    try:
        base = datetime.strptime(data, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400,
                            detail="Data invalida. Use AAAA-MM-DD.")
    inicio = base.replace(tzinfo=FUSO_BRASIL)
    return inicio, inicio + timedelta(days=1)


# ============================================================
# PAGINA INICIAL
# ============================================================

@app.get("/", response_class=HTMLResponse)
def inicio():
    hoje = datetime.now(FUSO_BRASIL).strftime("%Y-%m-%d")
    return f"""
    <html><body style="font-family: sans-serif; max-width: 640px;
         margin: 40px auto; line-height: 1.7;">
      <h2>API Apolo Solar v2</h2>
      <p>API de leitura do banco de monitoramento (schema v2).</p>
      <ul>
        <li><a href="/saude">/saude</a> &mdash; a API e o banco estao no ar?</li>
        <li><a href="/resumo">/resumo</a> &mdash; visao geral dos dados</li>
        <li><a href="/inversores">/inversores</a> &mdash; inversores cadastrados</li>
        <li><a href="/ultimas">/ultimas</a> &mdash; 20 leituras mais recentes</li>
        <li><a href="/checagem">/checagem</a> &mdash; procura problemas</li>
        <li>/dia/<b>AAAA-MM-DD</b> &mdash;
            ex: <a href="/dia/{hoje}">/dia/{hoje}</a></li>
        <li>/dia/<b>AAAA-MM-DD</b>/canais &mdash;
            ex: <a href="/dia/{hoje}/canais">/dia/{hoje}/canais</a></li>
      </ul>
      <p><a href="/docs">/docs</a> &mdash; documentacao interativa</p>
    </body></html>
    """


# ============================================================
# /saude
# ============================================================

@app.get("/saude")
def saude():
    """Verifica se a API responde e o banco esta acessivel."""
    tabelas = consultar(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
        "ORDER BY tablename"
    )
    return {
        "api": "no ar",
        "versao": "v2",
        "banco": "acessivel",
        "tabelas": [t["tablename"] for t in tabelas],
    }


# ============================================================
# /resumo
# ============================================================

@app.get("/resumo")
def resumo():
    """Numeros gerais: total de leituras, periodo, status."""
    total = um("SELECT COUNT(*) AS n FROM leitura")["n"]
    if total == 0:
        return {"aviso": "Banco sem leituras ainda."}

    periodo = um(
        "SELECT MIN(data_hora) AS inicio, MAX(data_hora) AS fim FROM leitura"
    )
    por_status = consultar(
        "SELECT status, COUNT(*) AS n FROM leitura "
        "GROUP BY status ORDER BY n DESC"
    )
    n_inv = um("SELECT COUNT(*) AS n FROM inversor")["n"]

    return {
        "total_leituras": total,
        "primeira_leitura": fmt(periodo["inicio"]),
        "ultima_leitura": fmt(periodo["fim"]),
        "inversores_cadastrados": n_inv,
        "leituras_por_status": {s["status"]: s["n"] for s in por_status},
    }


# ============================================================
# /inversores
# ============================================================

@app.get("/inversores")
def inversores():
    """Lista os inversores, o seu modelo e quantas leituras tem."""
    linhas = consultar("""
        SELECT i.idx, i.nome, i.serial_sn,
               m.nome AS modelo,
               m.num_mppt, m.num_string,
               COUNT(l.id) AS total_leituras,
               MAX(l.data_hora) AS ultima_leitura
        FROM inversor i
        JOIN modelo_inversor m ON m.id = i.modelo_id
        LEFT JOIN leitura l ON l.inversor_id = i.id
        GROUP BY i.id, i.idx, i.nome, i.serial_sn,
                 m.nome, m.num_mppt, m.num_string
        ORDER BY i.idx
    """)
    for l in linhas:
        l["ultima_leitura"] = fmt(l["ultima_leitura"])
    return linhas


# ============================================================
# /ultimas
# ============================================================

@app.get("/ultimas")
def ultimas():
    """As 20 leituras mais recentes."""
    linhas = consultar("""
        SELECT l.data_hora, i.nome AS inversor, l.status,
               l.pac_kw, l.dyield_kwh, l.tmod_c
        FROM leitura l
        JOIN inversor i ON i.id = l.inversor_id
        ORDER BY l.data_hora DESC, i.idx
        LIMIT 20
    """)
    for l in linhas:
        l["data_hora"] = fmt(l["data_hora"])
    return linhas


# ============================================================
# /dia/{data}
# ============================================================
# A API v1 devolvia TODAS as leituras cruas do dia e o
# dashboard processava. A v2 resume NO BANCO:
#   - a curva de potencia (soma de pac_kw por horario)
#   - o resumo (pico, energia)
# Assim trafega pouca coisa e a memoria nao estoura.
#
# MULTI-USINA: aceita ?usina=<slug> (default "pk") e filtra as
# leituras por usina.slug, para nao somar usinas diferentes.

@app.get("/dia/{data}")
def dia(data: str, usina: str = "pk"):
    """Resumo e curva de um dia (ex: /dia/2026-05-22?usina=ibiracu)."""
    inicio, fim = faixa_do_dia(data)

    # Curva de potencia: soma de pac_kw por horario, SOMENTE desta usina.
    # O agrupamento e feito pelo banco; volta uma linha por horario
    # (no maximo ~288 por dia), nao milhares.
    curva = consultar("""
        SELECT l.data_hora,
               SUM(l.pac_kw)     AS pac_total,
               SUM(l.dyield_kwh) AS dyield_total
        FROM leitura l
        JOIN inversor i ON i.id = l.inversor_id
        JOIN usina u    ON u.id = i.usina_id
        WHERE u.slug = %s
          AND l.data_hora >= %s AND l.data_hora < %s
        GROUP BY l.data_hora
        ORDER BY l.data_hora
    """, (usina, inicio, fim))

    if not curva:
        return {"data": data, "aviso": "Nenhuma leitura neste dia.",
                "curva": [], "resumo": None}

    # Pico de potencia = maior soma instantanea
    pico = max((c["pac_total"] or 0.0) for c in curva)

    # Energia do dia = maior dyield de cada inversor, somado (desta usina).
    # Tambem resumido pelo banco.
    energia = um("""
        SELECT COALESCE(SUM(maxdy), 0) AS total FROM (
            SELECT MAX(l.dyield_kwh) AS maxdy
            FROM leitura l
            JOIN inversor i ON i.id = l.inversor_id
            JOIN usina u    ON u.id = i.usina_id
            WHERE u.slug = %s
              AND l.data_hora >= %s AND l.data_hora < %s
            GROUP BY l.inversor_id
        ) sub
    """, (usina, inicio, fim))["total"]

    # Quantos inversores reportaram neste dia (desta usina)
    n_inv = um("""
        SELECT COUNT(DISTINCT l.inversor_id) AS n
        FROM leitura l
        JOIN inversor i ON i.id = l.inversor_id
        JOIN usina u    ON u.id = i.usina_id
        WHERE u.slug = %s
          AND l.data_hora >= %s AND l.data_hora < %s
    """, (usina, inicio, fim))["n"]

    # Formata os horarios da curva para o Brasil
    for c in curva:
        c["data_hora"]    = fmt(c["data_hora"])
        c["pac_total"]    = round(c["pac_total"] or 0.0, 3)
        c["dyield_total"] = round(c["dyield_total"] or 0.0, 3)

    return {
        "data": data,
        "resumo": {
            "horarios": len(curva),
            "inversores_no_dia": n_inv,
            "pico_potencia_kw": round(pico, 2),
            "energia_dia_kwh": round(float(energia), 2),
        },
        "curva": curva,
    }


# ============================================================
# /dia/{data}/canais
# ============================================================
# Os cards do dashboard mostram o ESTADO ATUAL de cada
# inversor — ou seja, apenas a leitura MAIS RECENTE.
# Portanto este endereco devolve, para cada inversor, somente
# a sua ultima leitura do dia (cabecalho + canais MPPT +
# canais string). Sao ~8 leituras, nao as ~2300 de um dia
# inteiro. Isso elimina o estouro de memoria.
#
# MULTI-USINA: aceita ?usina=<slug> (default "pk") e filtra por
# usina.slug, para nao misturar inversores de usinas diferentes
# (PK e Ibiracu tem ambos "Inversor 1", "Inversor 2"...).

@app.get("/dia/{data}/canais")
def dia_canais(data: str, usina: str = "pk"):
    """Canais da leitura mais recente de cada inversor no dia."""
    inicio, fim = faixa_do_dia(data)

    # 1) Para cada inversor DESTA usina, acha o id da sua leitura mais
    #    recente dentro do dia. DISTINCT ON resolve isso no banco.
    leituras = consultar("""
        SELECT DISTINCT ON (l.inversor_id)
               l.id, l.inversor_id, i.idx, i.nome AS inversor,
               l.data_hora, l.status,
               l.pac_kw, l.dyield_kwh, l.tyield_kwh,
               l.freq_hz, l.tmod_c, l.tamb_c, l.iso_kohm, l.pdc_kw
        FROM leitura l
        JOIN inversor i ON i.id = l.inversor_id
        JOIN usina u    ON u.id = i.usina_id
        WHERE u.slug = %s
          AND l.data_hora >= %s AND l.data_hora < %s
        ORDER BY l.inversor_id, l.data_hora DESC
    """, (usina, inicio, fim))

    if not leituras:
        return {"data": data, "aviso": "Nenhuma leitura neste dia.",
                "inversores": []}

    ids = [l["id"] for l in leituras]

    # 2) Canais MPPT dessas leituras (so as mais recentes)
    mppts = consultar("""
        SELECT leitura_id, mppt, tensao_v, corrente_a, potencia_w
        FROM leitura_mppt
        WHERE leitura_id = ANY(%s)
        ORDER BY leitura_id, mppt
    """, (ids,))

    # 3) Canais string dessas leituras
    strings = consultar("""
        SELECT leitura_id, string_num, mppt, corrente_a, potencia_w
        FROM leitura_string
        WHERE leitura_id = ANY(%s)
        ORDER BY leitura_id, string_num
    """, (ids,))

    # 4) Agrupa os canais por leitura
    mppt_por_leitura = {}
    for m in mppts:
        mppt_por_leitura.setdefault(m["leitura_id"], []).append({
            "mppt": m["mppt"], "tensao_v": m["tensao_v"],
            "corrente_a": m["corrente_a"], "potencia_w": m["potencia_w"],
        })
    string_por_leitura = {}
    for s in strings:
        string_por_leitura.setdefault(s["leitura_id"], []).append({
            "string_num": s["string_num"], "mppt": s["mppt"],
            "corrente_a": s["corrente_a"], "potencia_w": s["potencia_w"],
        })

    # 5) Monta a resposta: um objeto por inversor
    saida = []
    for l in leituras:
        saida.append({
            "idx": l["idx"],
            "inversor": l["inversor"],
            "data_hora": fmt(l["data_hora"]),
            "status": l["status"],
            "pac_kw": l["pac_kw"],
            "dyield_kwh": l["dyield_kwh"],
            "tyield_kwh": l["tyield_kwh"],
            "freq_hz": l["freq_hz"],
            "tmod_c": l["tmod_c"],
            "tamb_c": l["tamb_c"],
            "iso_kohm": l["iso_kohm"],
            "pdc_kw": l["pdc_kw"],
            "mppts": mppt_por_leitura.get(l["id"], []),
            "strings": string_por_leitura.get(l["id"], []),
        })
    saida.sort(key=lambda x: x["idx"])

    return {"data": data, "inversores": saida}


# ============================================================
# /dia/{data}/curva-inversores
# ============================================================
# Para o mini-grafico do card expandido: a curva de potencia
# (pac_kw) de CADA inversor ao longo do dia.
# E um endereco LEVE: traz apenas data_hora + pac_kw (sem os
# canais MPPT/string), entao sao algumas centenas de linhas,
# nao dezenas de milhares. Nao ha risco de memoria.
#
# MULTI-USINA: aceita ?usina=<slug> (default "pk") e filtra por
# usina.slug — senao a serie de "Inversor 1" misturaria PK e
# Ibiracu (nomes iguais entre usinas).

@app.get("/dia/{data}/curva-inversores")
def dia_curva_inversores(data: str, usina: str = "pk"):
    """Serie de pac_kw de cada inversor ao longo do dia."""
    inicio, fim = faixa_do_dia(data)

    linhas = consultar("""
        SELECT i.idx, i.nome AS inversor,
               l.data_hora, l.pac_kw
        FROM leitura l
        JOIN inversor i ON i.id = l.inversor_id
        JOIN usina u    ON u.id = i.usina_id
        WHERE u.slug = %s
          AND l.data_hora >= %s AND l.data_hora < %s
        ORDER BY i.idx, l.data_hora
    """, (usina, inicio, fim))

    if not linhas:
        return {"data": data, "aviso": "Nenhuma leitura neste dia.",
                "inversores": []}

    # Agrupa a serie por inversor
    por_inversor = {}
    for l in linhas:
        nome = l["inversor"]
        if nome not in por_inversor:
            por_inversor[nome] = {"idx": l["idx"], "nome": nome, "serie": []}
        por_inversor[nome]["serie"].append({
            "hora": fmt(l["data_hora"]),
            "pac_kw": l["pac_kw"] or 0.0,
        })

    saida = sorted(por_inversor.values(), key=lambda x: x["idx"])
    return {"data": data, "inversores": saida}


# ============================================================
# /mensal/{aaaa-mm}
# ============================================================
# Le da tabela resumo_dia (alimentada pelo resumidor diario).
# Devolve:
#   resumo:       totais do mes (energia, pico, dias com dados)
#   dias:         lista [{data, energia_kwh, pico_kw, pico_hora}] - 1 por dia
#   por_inversor: lista [{idx, nome, dias:[{data,energia_kwh,pico_kw}]}] - 1 por inv
#                 (para o "grafico em 8" do dashboard)

@app.get("/mensal/{aaaa_mm}")
def mensal(aaaa_mm: str, usina: str = "pk"):
    """Resumo de um mes (ex: /mensal/2026-05). ?usina=<slug> (default 'pk')."""
    try:
        ano, mes = aaaa_mm.split("-")
        ano_i, mes_i = int(ano), int(mes)
        if not (1 <= mes_i <= 12):
            raise ValueError("mes fora de 1-12")
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400,
                            detail="Formato invalido. Use AAAA-MM.")

    # Janela [inicio, fim) de datas
    from datetime import date as _date
    inicio = _date(ano_i, mes_i, 1)
    fim = _date(ano_i + (1 if mes_i == 12 else 0),
                1 if mes_i == 12 else mes_i + 1, 1)

    # ---- dias (curva do mes) ----
    # MULTI-USINA: filtra resumo_dia pela usina; sem isso PK e Ibiracu
    # seriam somados (resumo_dia tem uma linha por usina por dia).
    dias = consultar("""
        SELECT rd.data, rd.energia_kwh, rd.pac_medio_kw, rd.pico_kw,
               rd.pico_hora, rd.insolacao_h, rd.inversores_no_dia
        FROM resumo_dia rd
        JOIN usina u ON u.id = rd.usina_id
        WHERE u.slug = %s AND rd.data >= %s AND rd.data < %s
        ORDER BY rd.data
    """, (usina, inicio, fim))

    if not dias:
        return {"mes": aaaa_mm, "aviso": "Nenhum resumo para este mes.",
                "dias": [], "resumo": None, "por_inversor": []}

    # ---- resumo do mes ----
    energia_total = sum(float(d["energia_kwh"] or 0) for d in dias)
    pico_obj = max(dias, key=lambda d: float(d["pico_kw"] or 0))
    # soma de insolacao (ignora dias sem dado)
    insol_valores = [float(d["insolacao_h"]) for d in dias if d["insolacao_h"] is not None]
    insol_total = round(sum(insol_valores), 1) if insol_valores else None

    for d in dias:
        d["data"]         = d["data"].isoformat()
        d["energia_kwh"]  = float(d["energia_kwh"] or 0)
        d["pac_medio_kw"] = float(d["pac_medio_kw"]) if d["pac_medio_kw"] is not None else 0.0
        d["pico_kw"]      = float(d["pico_kw"] or 0)
        d["pico_hora"]    = d["pico_hora"].strftime("%H:%M") if d["pico_hora"] else None
        d["insolacao_h"]  = float(d["insolacao_h"]) if d["insolacao_h"] is not None else None

    # ---- por inversor (para o grafico em 8 + heatmap de disponibilidade) ----
    por_inv_linhas = consultar("""
        SELECT i.idx, i.nome, r.data, r.energia_kwh, r.pico_kw, r.pico_hora,
               r.disponibilidade, r.pac_medio_6_18_kw
        FROM resumo_dia_inversor r
        JOIN inversor i ON i.id = r.inversor_id
        JOIN usina u    ON u.id = i.usina_id
        WHERE u.slug = %s AND r.data >= %s AND r.data < %s
        ORDER BY i.idx, r.data
    """, (usina, inicio, fim))

    por_inv_map = {}
    for l in por_inv_linhas:
        nome = l["nome"]
        if nome not in por_inv_map:
            por_inv_map[nome] = {"idx": l["idx"], "nome": nome, "dias": []}
        por_inv_map[nome]["dias"].append({
            "data":            l["data"].isoformat(),
            "energia_kwh":     float(l["energia_kwh"] or 0),
            "pico_kw":         float(l["pico_kw"] or 0),
            "pico_hora":       l["pico_hora"].strftime("%H:%M") if l["pico_hora"] else None,
            "disponibilidade": float(l["disponibilidade"]) if l["disponibilidade"] is not None else 0.0,
            "pac_medio_kw":    float(l["pac_medio_6_18_kw"]) if l["pac_medio_6_18_kw"] is not None else 0.0,
        })
    por_inversor = sorted(por_inv_map.values(), key=lambda x: x["idx"])

    return {
        "mes": aaaa_mm,
        "resumo": {
            "energia_kwh":    round(energia_total, 2),
            "pico_kw":        float(pico_obj["pico_kw"]),
            "pico_data":      pico_obj["data"],
            "pico_hora":      pico_obj["pico_hora"],
            "insolacao_h":    insol_total,
            "dias_com_dados": len(dias),
        },
        "dias": dias,
        "por_inversor": por_inversor,
    }


# ============================================================
# /anual/{aaaa}
# ============================================================
# Agrega resumo_dia em 12 meses do ano. Devolve:
#   resumo:       totais do ano
#   meses:        lista [{mes:1..12, energia_kwh, pico_kw}]
#   por_inversor: lista [{idx, nome, meses:[{mes,energia_kwh,pico_kw}]}]

@app.get("/anual/{aaaa}")
def anual(aaaa: str, usina: str = "pk"):
    """Resumo de um ano (ex: /anual/2026). ?usina=<slug> (default 'pk')."""
    try:
        ano_i = int(aaaa)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400,
                            detail="Ano invalido. Use AAAA.")

    from datetime import date as _date
    inicio = _date(ano_i, 1, 1)
    fim    = _date(ano_i + 1, 1, 1)

    # ---- meses (curva do ano) ----
    # Agrega resumo_dia por mes, filtrando pela usina.
    meses = consultar("""
        SELECT EXTRACT(MONTH FROM rd.data)::int AS mes,
               SUM(rd.energia_kwh)              AS energia,
               MAX(rd.pico_kw)                  AS pico,
               AVG(rd.pac_medio_kw)             AS pac_medio,
               SUM(rd.insolacao_h)              AS insolacao,
               COUNT(*)                         AS dias_com_dados
        FROM resumo_dia rd
        JOIN usina u ON u.id = rd.usina_id
        WHERE u.slug = %s AND rd.data >= %s AND rd.data < %s
        GROUP BY EXTRACT(MONTH FROM rd.data)
        ORDER BY mes
    """, (usina, inicio, fim))

    if not meses:
        return {"ano": aaaa, "aviso": "Nenhum dado para este ano.",
                "meses": [], "resumo": None, "por_inversor": []}

    for m in meses:
        m["energia_kwh"]    = float(m.pop("energia") or 0)
        m["pico_kw"]        = float(m.pop("pico") or 0)
        pm = m.pop("pac_medio")
        m["pac_medio_kw"]   = float(pm) if pm is not None else 0.0
        ins = m.pop("insolacao")
        m["insolacao_h"]    = float(ins) if ins is not None else None
        m["dias_com_dados"] = m["dias_com_dados"]

    energia_ano = sum(m["energia_kwh"] for m in meses)
    pico_obj = max(meses, key=lambda m: m["pico_kw"])

    # ---- por inversor ----
    por_inv_linhas = consultar("""
        SELECT i.idx, i.nome,
               EXTRACT(MONTH FROM r.data)::int AS mes,
               SUM(r.energia_kwh)              AS energia,
               MAX(r.pico_kw)                  AS pico,
               AVG(r.pac_medio_6_18_kw)        AS pac_medio
        FROM resumo_dia_inversor r
        JOIN inversor i ON i.id = r.inversor_id
        JOIN usina u    ON u.id = i.usina_id
        WHERE u.slug = %s AND r.data >= %s AND r.data < %s
        GROUP BY i.idx, i.nome, EXTRACT(MONTH FROM r.data)
        ORDER BY i.idx, mes
    """, (usina, inicio, fim))

    por_inv_map = {}
    for l in por_inv_linhas:
        nome = l["nome"]
        if nome not in por_inv_map:
            por_inv_map[nome] = {"idx": l["idx"], "nome": nome, "meses": []}
        por_inv_map[nome]["meses"].append({
            "mes":          l["mes"],
            "energia_kwh":  float(l["energia"] or 0),
            "pico_kw":      float(l["pico"] or 0),
            "pac_medio_kw": float(l["pac_medio"]) if l["pac_medio"] is not None else 0.0,
        })
    por_inversor = sorted(por_inv_map.values(), key=lambda x: x["idx"])

    return {
        "ano": aaaa,
        "resumo": {
            "energia_kwh":     round(energia_ano, 2),
            "pico_kw":         pico_obj["pico_kw"],
            "pico_mes":        pico_obj["mes"],
            "meses_com_dados": len(meses),
        },
        "meses": meses,
        "por_inversor": por_inversor,
    }


# ============================================================
# /checagem
# ============================================================

@app.get("/checagem")
def checagem():
    """Varre os dados procurando sinais de problema."""
    problemas = []
    total = um("SELECT COUNT(*) AS n FROM leitura")["n"]
    if total == 0:
        return {"aviso": "Banco vazio — nada a checar."}

    erros = um(
        "SELECT COUNT(*) AS n FROM leitura "
        "WHERE status IN ('ERRO', 'SEM_DADOS')"
    )["n"]
    if erros > 0:
        problemas.append(
            f"{erros} leitura(s) com status ERRO ou SEM_DADOS "
            f"({erros * 100 // total}% do total).")

    pac_neg = um(
        "SELECT COUNT(*) AS n FROM leitura WHERE pac_kw < 0"
    )["n"]
    if pac_neg > 0:
        problemas.append(f"{pac_neg} leitura(s) com potencia NEGATIVA.")

    pac_alta = um(
        "SELECT COUNT(*) AS n FROM leitura WHERE pac_kw > 200"
    )["n"]
    if pac_alta > 0:
        problemas.append(
            f"{pac_alta} leitura(s) com potencia acima de 200 kW.")

    pv_neg = um(
        "SELECT COUNT(*) AS n FROM leitura_string "
        "WHERE corrente_a < -0.05"
    )["n"]
    if pv_neg > 0:
        problemas.append(
            f"{pv_neg} canal(is) de string com corrente negativa.")

    return {
        "total_leituras_analisadas": total,
        "problemas_encontrados": len(problemas),
        "detalhes": problemas if problemas
                    else ["Nenhum problema obvio encontrado."],
    }


# ============================================================
# EXECUCAO — o Railway define a porta na variavel PORT
# ============================================================

if __name__ == "__main__":
    import uvicorn
    porta = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=porta)
