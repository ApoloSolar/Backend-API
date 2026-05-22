# -*- coding: utf-8 -*-
"""
============================================================
  API — APOLO SOLAR  (Railway / PostgreSQL)
============================================================
API que le o banco PostgreSQL e serve os dados para o
dashboard, em formato JSON.

Roda como servico web no Railway, sempre disponivel.
O dashboard (no GitHub Pages) faz fetch nos enderecos desta API.

DIFERENCA PARA a api_teste.py:
  - Le PostgreSQL (psycopg), nao SQLite
  - Corrige o fuso: o Railway roda em UTC; os horarios sao
    convertidos para o horario do Brasil (America/Sao_Paulo)
  - Preparada para rodar no Railway (le porta do ambiente)

CREDENCIAIS: a DATABASE_URL vem de variavel de ambiente.

ENDERECOS:
  /                 -> pagina inicial com os links
  /saude            -> a API e o banco estao no ar?
  /resumo           -> visao geral: total, periodo, status
  /inversores       -> inversores e quantas leituras cada um tem
  /ultimas          -> 20 leituras mais recentes
  /dia/{data}       -> leituras de um dia (AAAA-MM-DD)
  /checagem         -> procura problemas nos dados
============================================================
"""

import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

import psycopg
from psycopg.rows import dict_row


# ============================================================
# CONFIGURACAO
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Variavel DATABASE_URL nao configurada.")

# O Railway roda em UTC. O Brasil (Espirito Santo) e UTC-3.
# Aplicamos esse deslocamento para exibir os horarios corretos.
FUSO_BRASIL = timezone(timedelta(hours=-3))

INTERVALO_MIN = 5   # passo esperado entre leituras

app = FastAPI(title="API Apolo Solar")

# CORS — permite que o dashboard (noutro endereco) chame a API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# ACESSO AO BANCO
# ============================================================

def consultar(sql, params=()):
    """Executa um SELECT e devolve as linhas como lista de dicionarios."""
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=15) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, params)
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"Erro ao consultar o banco: {e}")


def fmt(dt):
    """Formata um datetime do banco para texto 'AAAA-MM-DD HH:MM'.
    Os horarios sao gravados em UTC; convertemos para o Brasil."""
    if dt is None:
        return None
    # Se o datetime nao tem fuso, assumimos que esta em UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(FUSO_BRASIL).strftime("%Y-%m-%d %H:%M")


# ============================================================
# PAGINA INICIAL
# ============================================================

@app.get("/", response_class=HTMLResponse)
def inicio():
    hoje = datetime.now(FUSO_BRASIL).strftime("%Y-%m-%d")
    return f"""
    <html><body style="font-family: sans-serif; max-width: 640px;
         margin: 40px auto; line-height: 1.7;">
      <h2>API Apolo Solar</h2>
      <p>API de leitura do banco de dados de monitoramento.</p>
      <ul>
        <li><a href="/saude">/saude</a> &mdash; a API e o banco estao no ar?</li>
        <li><a href="/resumo">/resumo</a> &mdash; visao geral dos dados</li>
        <li><a href="/inversores">/inversores</a> &mdash; inversores cadastrados</li>
        <li><a href="/ultimas">/ultimas</a> &mdash; 20 leituras mais recentes</li>
        <li><a href="/checagem">/checagem</a> &mdash; procura problemas</li>
        <li>/dia/<b>AAAA-MM-DD</b> &mdash;
            ex: <a href="/dia/{hoje}">/dia/{hoje}</a></li>
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
        "banco": "acessivel",
        "tabelas": [t["tablename"] for t in tabelas],
    }


# ============================================================
# /resumo
# ============================================================

@app.get("/resumo")
def resumo():
    """Numeros gerais: total de leituras, periodo, status."""
    total = consultar("SELECT COUNT(*) AS n FROM leitura")[0]["n"]
    if total == 0:
        return {"aviso": "Banco sem leituras ainda."}

    periodo = consultar(
        "SELECT MIN(data_hora) AS inicio, MAX(data_hora) AS fim FROM leitura"
    )[0]
    por_status = consultar(
        "SELECT status, COUNT(*) AS n FROM leitura "
        "GROUP BY status ORDER BY n DESC"
    )
    n_inv = consultar("SELECT COUNT(*) AS n FROM inversor")[0]["n"]

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
    """Lista os inversores e quantas leituras cada um tem."""
    linhas = consultar("""
        SELECT i.idx, i.nome, i.serial_sn,
               COUNT(l.id) AS total_leituras,
               MAX(l.data_hora) AS ultima_leitura
        FROM inversor i
        LEFT JOIN leitura l ON l.inversor_id = i.id
        GROUP BY i.id, i.idx, i.nome, i.serial_sn
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

@app.get("/dia/{data}")
def dia(data: str):
    """Leituras de um dia inteiro (ex: /dia/2026-05-22), com resumo."""
    try:
        datetime.strptime(data, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400,
                            detail="Data invalida. Use AAAA-MM-DD.")

    # A coluna data_hora esta em UTC; o dia pedido e em horario do Brasil.
    # Convertendo: o dia brasileiro vai de 03:00 UTC ate 03:00 UTC do dia seguinte.
    inicio = datetime.strptime(data, "%Y-%m-%d").replace(tzinfo=FUSO_BRASIL)
    fim = inicio + timedelta(days=1)

    leituras = consultar("""
        SELECT l.data_hora, i.nome AS inversor, i.idx, l.status,
               l.pac_kw, l.dyield_kwh, l.tyield_kwh,
               l.freq_hz, l.tmod_c, l.tamb_c
        FROM leitura l
        JOIN inversor i ON i.id = l.inversor_id
        WHERE l.data_hora >= %s AND l.data_hora < %s
        ORDER BY l.data_hora, i.idx
    """, (inicio, fim))

    if not leituras:
        return {"data": data, "aviso": "Nenhuma leitura neste dia.",
                "leituras": []}

    # Pico de potencia = maior soma de pac_kw entre os horarios
    por_horario = {}
    for l in leituras:
        h = l["data_hora"]
        por_horario[h] = por_horario.get(h, 0.0) + (l["pac_kw"] or 0.0)
    pico = max(por_horario.values()) if por_horario else 0.0

    # Energia do dia = maior DYield de cada inversor, somado
    energia = consultar("""
        SELECT COALESCE(SUM(maxdy), 0) AS total FROM (
            SELECT MAX(l.dyield_kwh) AS maxdy
            FROM leitura l
            WHERE l.data_hora >= %s AND l.data_hora < %s
            GROUP BY l.inversor_id
        ) sub
    """, (inicio, fim))[0]["total"]

    # Converte os horarios para o Brasil
    for l in leituras:
        l["data_hora"] = fmt(l["data_hora"])

    return {
        "data": data,
        "resumo": {
            "total_leituras": len(leituras),
            "horarios_distintos": len(por_horario),
            "pico_potencia_kw": round(pico, 2),
            "energia_dia_kwh": round(float(energia), 2),
        },
        "leituras": leituras,
    }


@app.get("/dia/{data}/canais")
def dia_canais(data: str):
    """Canais (MPPT e strings PV) das leituras de um dia.
    Separado de /dia para manter cada resposta enxuta. O dashboard
    usa isto para montar os cards detalhados de cada inversor."""
    try:
        datetime.strptime(data, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400,
                            detail="Data invalida. Use AAAA-MM-DD.")

    inicio = datetime.strptime(data, "%Y-%m-%d").replace(tzinfo=FUSO_BRASIL)
    fim = inicio + timedelta(days=1)

    linhas = consultar("""
        SELECT l.data_hora, i.idx, i.nome AS inversor,
               c.tipo, c.canal, c.tensao_v, c.corrente_a, c.potencia_w
        FROM leitura_canal c
        JOIN leitura  l ON l.id = c.leitura_id
        JOIN inversor i ON i.id = l.inversor_id
        WHERE l.data_hora >= %s AND l.data_hora < %s
        ORDER BY l.data_hora, i.idx, c.tipo, c.canal
    """, (inicio, fim))

    for l in linhas:
        l["data_hora"] = fmt(l["data_hora"])
    return {"data": data, "canais": linhas}


# ============================================================
# /checagem
# ============================================================

@app.get("/checagem")
def checagem():
    """Varre os dados procurando sinais de problema."""
    problemas = []
    total = consultar("SELECT COUNT(*) AS n FROM leitura")[0]["n"]
    if total == 0:
        return {"aviso": "Banco vazio — nada a checar."}

    erros = consultar(
        "SELECT COUNT(*) AS n FROM leitura "
        "WHERE status IN ('ERRO', 'SEM_DADOS')"
    )[0]["n"]
    if erros > 0:
        problemas.append(
            f"{erros} leitura(s) com status ERRO ou SEM_DADOS "
            f"({erros * 100 // total}% do total).")

    pac_neg = consultar(
        "SELECT COUNT(*) AS n FROM leitura WHERE pac_kw < 0"
    )[0]["n"]
    if pac_neg > 0:
        problemas.append(f"{pac_neg} leitura(s) com potencia NEGATIVA.")

    pac_alta = consultar(
        "SELECT COUNT(*) AS n FROM leitura WHERE pac_kw > 200"
    )[0]["n"]
    if pac_alta > 0:
        problemas.append(
            f"{pac_alta} leitura(s) com potencia acima de 200 kW.")

    pv_neg = consultar(
        "SELECT COUNT(*) AS n FROM leitura_canal "
        "WHERE tipo = 'PV' AND corrente_a < -0.05"
    )[0]["n"]
    if pv_neg > 0:
        problemas.append(
            f"{pv_neg} canal(is) de string PV com corrente negativa.")

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
