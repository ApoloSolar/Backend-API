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

CREDENCIAIS: DATABASE_URL vem de variavel de ambiente.

ENDERECOS:
  /                      -> pagina inicial com os links
  /saude                 -> a API e o banco estao no ar?
  /resumo                -> visao geral: total, periodo, status
  /inversores            -> inversores e contagem de leituras
  /ultimas               -> 20 leituras mais recentes
  /dia/{data}            -> resumo + curva do dia (AAAA-MM-DD)
  /dia/{data}/canais     -> canais da leitura mais recente
                            de cada inversor naquele dia
  /checagem              -> procura problemas nos dados
============================================================
"""

import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

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

@app.get("/dia/{data}")
def dia(data: str):
    """Resumo e curva de um dia (ex: /dia/2026-05-22)."""
    inicio, fim = faixa_do_dia(data)

    # Curva de potencia: soma de pac_kw por horario.
    # O agrupamento e feito pelo banco; volta uma linha por horario
    # (no maximo ~288 por dia), nao milhares.
    curva = consultar("""
        SELECT l.data_hora,
               SUM(l.pac_kw)     AS pac_total,
               SUM(l.dyield_kwh) AS dyield_total
        FROM leitura l
        WHERE l.data_hora >= %s AND l.data_hora < %s
        GROUP BY l.data_hora
        ORDER BY l.data_hora
    """, (inicio, fim))

    if not curva:
        return {"data": data, "aviso": "Nenhuma leitura neste dia.",
                "curva": [], "resumo": None}

    # Pico de potencia = maior soma instantanea
    pico = max((c["pac_total"] or 0.0) for c in curva)

    # Energia do dia = maior dyield de cada inversor, somado.
    # Tambem resumido pelo banco.
    energia = um("""
        SELECT COALESCE(SUM(maxdy), 0) AS total FROM (
            SELECT MAX(l.dyield_kwh) AS maxdy
            FROM leitura l
            WHERE l.data_hora >= %s AND l.data_hora < %s
            GROUP BY l.inversor_id
        ) sub
    """, (inicio, fim))["total"]

    # Quantos inversores reportaram neste dia
    n_inv = um("""
        SELECT COUNT(DISTINCT inversor_id) AS n
        FROM leitura
        WHERE data_hora >= %s AND data_hora < %s
    """, (inicio, fim))["n"]

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

@app.get("/dia/{data}/canais")
def dia_canais(data: str):
    """Canais da leitura mais recente de cada inversor no dia."""
    inicio, fim = faixa_do_dia(data)

    # 1) Para cada inversor, acha o id da sua leitura mais recente
    #    dentro do dia. DISTINCT ON resolve isso no banco.
    leituras = consultar("""
        SELECT DISTINCT ON (l.inversor_id)
               l.id, l.inversor_id, i.idx, i.nome AS inversor,
               l.data_hora, l.status,
               l.pac_kw, l.dyield_kwh, l.tyield_kwh,
               l.freq_hz, l.tmod_c, l.tamb_c, l.iso_kohm, l.pdc_kw
        FROM leitura l
        JOIN inversor i ON i.id = l.inversor_id
        WHERE l.data_hora >= %s AND l.data_hora < %s
        ORDER BY l.inversor_id, l.data_hora DESC
    """, (inicio, fim))

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
