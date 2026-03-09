from flask import Flask, render_template, request, redirect, jsonify, session, url_for, send_from_directory
from banco import criar_tabelas, conectar
from datetime import datetime, timedelta
import sqlite3
import os
import uuid
import base64
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "centralmarket-dev-key-change")

ROLE_LEVEL = {
    "funcionario": 1,
    "gerente": 2,
    "proprietario": 3
}

GERENTE_ENDPOINTS = {
    "abrir_caixa",
    "fechar_caixa",
    "tela_caixa",
    "caixa_pedidos",
    "pagar_pedido",
    "cancelar_pedido",
    "painel_caixa",
    "categorias",
    "editar_categoria",
    "fornecedores",
    "editar_fornecedor",
    "excluir_fornecedor",
    "produtos",
    "editar",
    "financeiro",
    "excluir_lancamento_financeiro",
    "criar_conta_financeira",
    "baixar_conta_financeira",
    "estornar_conta_financeira",
    "excluir_conta_financeira",
    "salvar_meta_financeira",
}

PROPRIETARIO_ENDPOINTS = {
    "funcionarios",
    "editar_funcionario",
    "excluir_funcionario",
    "loja_config",
    "config_descontos",
}

AUTH_PUBLIC_ENDPOINTS = {
    "login",
    "logout",
    "static",
    "app_tecnico_manifest",
    "app_tecnico_sw",
}

AUTH_SCHEMA_READY = False
OS_MOBILE_SCHEMA_READY = False

UPLOAD_OS_DIR = os.path.join("static", "uploads_os")

def get_db():
    conn = sqlite3.connect("database.db", timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def parse_float_br(valor_raw, default=0.0):
    s = str(valor_raw or "").strip()
    if not s:
        return float(default)

    s = s.replace("R$", "").replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        s = s.replace(",", ".")

    try:
        return float(s)
    except (TypeError, ValueError):
        return float(default)


def ensure_movimentos_cancelamento_columns():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(movimentos)")
    cols = {c["name"] for c in cursor.fetchall()}

    if "motivo_cancelamento" not in cols:
        cursor.execute("ALTER TABLE movimentos ADD COLUMN motivo_cancelamento TEXT")
    if "data_cancelamento" not in cols:
        cursor.execute("ALTER TABLE movimentos ADD COLUMN data_cancelamento TEXT")

    conn.commit()
    conn.close()


def ensure_movimentos_desconto_columns():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(movimentos)")
    cols = {c["name"] for c in cursor.fetchall()}

    if "desconto_valor" not in cols:
        cursor.execute("ALTER TABLE movimentos ADD COLUMN desconto_valor REAL DEFAULT 0")
    if "desconto_tipo" not in cols:
        cursor.execute("ALTER TABLE movimentos ADD COLUMN desconto_tipo TEXT")
    if "autorizador_id" not in cols:
        cursor.execute("ALTER TABLE movimentos ADD COLUMN autorizador_id INTEGER")

    conn.commit()
    conn.close()


def ensure_movimentos_troco_columns():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(movimentos)")
    cols = {c["name"] for c in cursor.fetchall()}
    if "valor_recebido" not in cols:
        cursor.execute("ALTER TABLE movimentos ADD COLUMN valor_recebido REAL DEFAULT 0")
    if "troco" not in cols:
        cursor.execute("ALTER TABLE movimentos ADD COLUMN troco REAL DEFAULT 0")
    conn.commit()
    conn.close()


def ensure_desconto_config_table():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS desconto_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            desconto_livre_percent REAL DEFAULT 0,
            desconto_livre_valor REAL DEFAULT 0,
            limite_funcionario_percent REAL DEFAULT 5,
            limite_funcionario_valor REAL DEFAULT 50,
            limite_gerente_percent REAL DEFAULT 15,
            limite_gerente_valor REAL DEFAULT 200,
            limite_proprietario_percent REAL DEFAULT 100,
            limite_proprietario_valor REAL DEFAULT 999999
        )
    """)
    cursor.execute("PRAGMA table_info(desconto_config)")
    cols = {c["name"] for c in cursor.fetchall()}
    if "limite_funcionario_percent" not in cols:
        cursor.execute("ALTER TABLE desconto_config ADD COLUMN limite_funcionario_percent REAL DEFAULT 5")
    if "limite_funcionario_valor" not in cols:
        cursor.execute("ALTER TABLE desconto_config ADD COLUMN limite_funcionario_valor REAL DEFAULT 50")

    cursor.execute("SELECT COUNT(*) FROM desconto_config")
    total = cursor.fetchone()[0]
    if total == 0:
        cursor.execute("""
            INSERT INTO desconto_config (
                desconto_livre_percent, desconto_livre_valor,
                limite_funcionario_percent, limite_funcionario_valor,
                limite_gerente_percent, limite_gerente_valor,
                limite_proprietario_percent, limite_proprietario_valor
            ) VALUES (0, 0, 5, 50, 15, 200, 100, 999999)
        """)
    conn.commit()
    conn.close()


def get_desconto_config(cursor):
    cursor.execute("SELECT * FROM desconto_config WHERE id=1")
    cfg = cursor.fetchone()
    if not cfg:
        return {
            "desconto_livre_percent": 0.0,
            "desconto_livre_valor": 0.0,
            "limite_funcionario_percent": 5.0,
            "limite_funcionario_valor": 50.0,
            "limite_gerente_percent": 15.0,
            "limite_gerente_valor": 200.0,
            "limite_proprietario_percent": 100.0,
            "limite_proprietario_valor": 999999.0,
        }
    return cfg


def ensure_loja_config_table():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loja_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_fantasia TEXT,
            razao_social TEXT,
            cnpj TEXT,
            telefone TEXT,
            endereco TEXT,
            numero TEXT,
            bairro TEXT,
            cidade TEXT,
            mensagem_cupom TEXT
        )
    """)

    cursor.execute("SELECT COUNT(*) FROM loja_config")
    total = cursor.fetchone()[0]
    if total == 0:
        cursor.execute("""
            INSERT INTO loja_config (
                nome_fantasia, razao_social, cnpj, telefone,
                endereco, numero, bairro, cidade, mensagem_cupom
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "Sua Loja",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "Obrigado pela preferencia!"
        ))

    conn.commit()
    conn.close()


def ensure_financeiro_table():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financeiro_lancamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT NOT NULL,
            categoria TEXT,
            descricao TEXT,
            valor REAL NOT NULL,
            data_lancamento TEXT NOT NULL,
            caixa_id INTEGER,
            funcionario_id INTEGER,
            origem TEXT DEFAULT 'MANUAL',
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def ensure_contas_financeiras_table():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contas_financeiras (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT NOT NULL,
            categoria TEXT,
            descricao TEXT,
            valor REAL NOT NULL,
            data_emissao TEXT NOT NULL,
            data_vencimento TEXT NOT NULL,
            status TEXT DEFAULT 'PENDENTE',
            data_pagamento TEXT,
            forma_pagamento TEXT,
            observacao TEXT,
            funcionario_id INTEGER,
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("PRAGMA table_info(contas_financeiras)")
    cols = {c["name"] for c in cursor.fetchall()}
    if "forma_pagamento" not in cols:
        cursor.execute("ALTER TABLE contas_financeiras ADD COLUMN forma_pagamento TEXT")
    if "observacao" not in cols:
        cursor.execute("ALTER TABLE contas_financeiras ADD COLUMN observacao TEXT")
    if "funcionario_id" not in cols:
        cursor.execute("ALTER TABLE contas_financeiras ADD COLUMN funcionario_id INTEGER")

    conn.commit()
    conn.close()


def ensure_financeiro_metas_table():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financeiro_metas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ano_mes TEXT NOT NULL UNIQUE,
            meta_receita REAL DEFAULT 0,
            meta_resultado REAL DEFAULT 0,
            atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP,
            funcionario_id INTEGER
        )
    """)
    conn.commit()
    conn.close()


def ensure_funcionarios_auth_columns():
    global AUTH_SCHEMA_READY
    if AUTH_SCHEMA_READY:
        return

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(funcionarios)")
    cols = {c["name"] for c in cursor.fetchall()}

    if "senha_hash" not in cols:
        cursor.execute("ALTER TABLE funcionarios ADD COLUMN senha_hash TEXT")
    if "perfil" not in cols:
        cursor.execute("ALTER TABLE funcionarios ADD COLUMN perfil TEXT DEFAULT 'funcionario'")

    cursor.execute("""
        UPDATE funcionarios
        SET perfil = CASE
            WHEN LOWER(COALESCE(cargo, '')) IN ('administrador', 'proprietario') THEN 'proprietario'
            WHEN LOWER(COALESCE(cargo, '')) = 'gerente' THEN 'gerente'
            ELSE 'funcionario'
        END
        WHERE perfil IS NULL OR TRIM(perfil) = ''
    """)

    cursor.execute("SELECT COUNT(*) FROM funcionarios WHERE perfil='proprietario'")
    total_prop = cursor.fetchone()[0]

    if total_prop == 0:
        cursor.execute("""
            INSERT INTO funcionarios
            (nome, cpf, cargo, funcao, status, senha_hash, perfil)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            "Proprietario Master",
            "00000000000",
            "Administrador",
            "Sistema",
            "Ativo",
            generate_password_hash("admin123"),
            "proprietario"
        ))

    conn.commit()
    conn.close()
    AUTH_SCHEMA_READY = True


def ensure_ordem_servico_mobile_columns():
    global OS_MOBILE_SCHEMA_READY
    if OS_MOBILE_SCHEMA_READY:
        return

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(ordem_servico)")
    cols = {c["name"] for c in cursor.fetchall()}

    if "checklist_json" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN checklist_json TEXT")
    if "assinatura_nome" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN assinatura_nome TEXT")
    if "assinatura_data" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN assinatura_data TEXT")
    if "assinatura_imagem" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN assinatura_imagem TEXT")
    if "movimento_id" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN movimento_id INTEGER")
    if "foto_inicio" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN foto_inicio TEXT")
    if "foto_fim" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN foto_fim TEXT")

    conn.commit()
    conn.close()
    os.makedirs(UPLOAD_OS_DIR, exist_ok=True)
    OS_MOBILE_SCHEMA_READY = True


def nivel_usuario():
    role = (session.get("user_role") or "funcionario").lower()
    return ROLE_LEVEL.get(role, 1)


def resposta_acesso_negado(msg):
    wants_json = request.is_json or "application/json" in (request.headers.get("Accept") or "")
    if wants_json:
        return jsonify({"ok": False, "erro": msg}), 403
    return msg, 403


@app.before_request
def validar_acesso():
    ensure_funcionarios_auth_columns()

    endpoint = request.endpoint or ""
    if endpoint in AUTH_PUBLIC_ENDPOINTS:
        return None
    if endpoint.startswith("static"):
        return None

    if "user_id" not in session:
        return redirect(url_for("login", next=request.path))

    if endpoint in PROPRIETARIO_ENDPOINTS and (session.get("user_role") != "proprietario"):
        return resposta_acesso_negado("Acesso permitido apenas para proprietario.")

    if endpoint in GERENTE_ENDPOINTS and nivel_usuario() < ROLE_LEVEL["gerente"]:
        return resposta_acesso_negado("Acesso permitido para gerente ou proprietario.")

    return None


@app.context_processor
def injetar_usuario():
    return {
        "usuario_logado": session.get("user_nome"),
        "perfil_logado": session.get("user_role"),
    }

# =================================================
# rota dashboard
# =================================================
@app.route("/login", methods=["GET", "POST"])
def login():
    ensure_funcionarios_auth_columns()
    erro = ""

    if request.method == "POST":
        cpf = "".join(ch for ch in (request.form.get("cpf") or "") if ch.isdigit())
        senha = request.form.get("senha") or ""

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, nome, senha_hash, perfil, status
            FROM funcionarios
            WHERE cpf=?
            LIMIT 1
        """, (cpf,))
        user = cursor.fetchone()
        conn.close()

        if not user:
            erro = "Usuario nao encontrado."
        elif (user["status"] or "Ativo") != "Ativo":
            erro = "Usuario inativo."
        elif not user["senha_hash"] or not check_password_hash(user["senha_hash"], senha):
            erro = "Senha invalida."
        else:
            session.clear()
            session["user_id"] = user["id"]
            session["user_nome"] = user["nome"]
            session["user_role"] = (user["perfil"] or "funcionario").lower()

            prox = request.args.get("next") or "/"
            if not prox.startswith("/"):
                prox = "/"
            return redirect(prox)

    return render_template("login.html", erro=erro)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/app_tecnico")
def app_tecnico():
    ensure_ordem_servico_mobile_columns()
    return render_template("app_tecnico.html")


@app.route("/app_tecnico/atendimento/<int:os_id>")
def app_tecnico_atendimento(os_id):
    ensure_ordem_servico_mobile_columns()
    return render_template("app_tecnico_atendimento.html", os_id=os_id)


@app.route("/app_tecnico/manifest.webmanifest")
def app_tecnico_manifest():
    return send_from_directory("static", "manifest_tecnico.webmanifest")


@app.route("/app_tecnico/sw.js")
def app_tecnico_sw():
    return send_from_directory("static", "sw_tecnico.js")


@app.route("/api/tecnico/agenda_hoje")
def api_tecnico_agenda_hoje():
    ensure_ordem_servico_mobile_columns()
    tecnico_id = session.get("user_id")
    if not tecnico_id:
        return jsonify({"ok": False, "erro": "Sessao expirada."}), 401

    hoje = datetime.now().strftime("%Y-%m-%d")
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            os.id,
            os.servico_nome,
            os.data_agendamento,
            os.hora,
            os.status,
            os.defeito_reclamado,
            os.produto_defeito,
            os.observacao,
            os.checklist_json,
            os.assinatura_nome,
            os.assinatura_data,
            os.assinatura_imagem,
            os.movimento_id,
            c.nome AS cliente,
            c.telefone,
            c.endereco,
            c.numero,
            c.bairro,
            c.cidade
        FROM ordem_servico os
        LEFT JOIN clientes c ON os.cliente_id = c.id
        WHERE os.tecnico_id = ?
          AND os.data_agendamento = ?
        ORDER BY os.hora
    """, (tecnico_id, hoje))

    ordens = cursor.fetchall()

    itens_por_os = {}
    if ordens:
        ids = [o["id"] for o in ordens]
        placeholders = ",".join(["?"] * len(ids))
        cursor.execute(f"""
            SELECT os_id, descricao, quantidade, valor, subtotal
            FROM itens_ordem_servico
            WHERE os_id IN ({placeholders})
            ORDER BY id
        """, ids)
        itens_rows = cursor.fetchall()
        for it in itens_rows:
            os_id = it["os_id"]
            if os_id not in itens_por_os:
                itens_por_os[os_id] = []
            itens_por_os[os_id].append({
                "descricao": it["descricao"],
                "quantidade": it["quantidade"],
                "valor": float(it["valor"] or 0),
                "subtotal": float(it["subtotal"] or 0),
            })

    conn.close()

    return jsonify({
        "ok": True,
        "hoje": hoje,
        "ordens": [
            {
                "id": o["id"],
                "servico_nome": o["servico_nome"],
                "data_agendamento": o["data_agendamento"],
                "hora": o["hora"],
                "status": o["status"],
                "defeito_reclamado": o["defeito_reclamado"],
                "produto_defeito": o["produto_defeito"],
                "observacao": o["observacao"],
                "checklist_json": o["checklist_json"],
                "assinatura_nome": o["assinatura_nome"],
                "assinatura_data": o["assinatura_data"],
                "assinatura_imagem": o["assinatura_imagem"],
                "movimento_id": o["movimento_id"],
                "itens": itens_por_os.get(o["id"], []),
                "cliente": o["cliente"],
                "telefone": o["telefone"],
                "endereco": o["endereco"],
                "numero": o["numero"],
                "bairro": o["bairro"],
                "cidade": o["cidade"],
            }
            for o in ordens
        ]
    })


@app.route("/api/tecnico/ordem/<int:os_id>")
def api_tecnico_ordem(os_id):
    ensure_ordem_servico_mobile_columns()
    tecnico_id = session.get("user_id")
    if not tecnico_id:
        return jsonify({"ok": False, "erro": "Sessao expirada."}), 401

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            os.id,
            os.servico_nome,
            os.data_agendamento,
            os.hora,
            os.status,
            os.defeito_reclamado,
            os.produto_defeito,
            os.observacao,
            os.assinatura_nome,
            os.assinatura_data,
            os.assinatura_imagem,
            os.movimento_id,
            c.nome AS cliente,
            c.telefone,
            c.endereco,
            c.numero,
            c.bairro,
            c.cidade
        FROM ordem_servico os
        LEFT JOIN clientes c ON os.cliente_id = c.id
        WHERE os.id = ? AND os.tecnico_id = ?
        LIMIT 1
    """, (os_id, tecnico_id))
    o = cursor.fetchone()
    if not o:
        conn.close()
        return jsonify({"ok": False, "erro": "Ordem nao encontrada para este tecnico."}), 404

    cursor.execute("""
        SELECT descricao, quantidade, valor, subtotal
        FROM itens_ordem_servico
        WHERE os_id = ?
        ORDER BY id
    """, (os_id,))
    itens = cursor.fetchall()
    conn.close()

    return jsonify({
        "ok": True,
        "ordem": {
            "id": o["id"],
            "servico_nome": o["servico_nome"],
            "data_agendamento": o["data_agendamento"],
            "hora": o["hora"],
            "status": o["status"],
            "defeito_reclamado": o["defeito_reclamado"],
            "produto_defeito": o["produto_defeito"],
            "observacao": o["observacao"],
            "assinatura_nome": o["assinatura_nome"],
            "assinatura_data": o["assinatura_data"],
            "assinatura_imagem": o["assinatura_imagem"],
            "movimento_id": o["movimento_id"],
            "cliente": o["cliente"],
            "telefone": o["telefone"],
            "endereco": o["endereco"],
            "numero": o["numero"],
            "bairro": o["bairro"],
            "cidade": o["cidade"],
            "itens": [
                {
                    "descricao": it["descricao"],
                    "quantidade": it["quantidade"],
                    "valor": float(it["valor"] or 0),
                    "subtotal": float(it["subtotal"] or 0),
                }
                for it in itens
            ]
        }
    })


@app.route("/api/tecnico/ordem/<int:os_id>/status", methods=["POST"])
def api_tecnico_atualizar_status(os_id):
    ensure_ordem_servico_mobile_columns()
    tecnico_id = session.get("user_id")
    if not tecnico_id:
        return jsonify({"ok": False, "erro": "Sessao expirada."}), 401

    dados = request.get_json(silent=True) or {}
    novo_status = (dados.get("status") or "").strip()
    status_validos = {"Agendado", "Em Atendimento", "Finalizado"}
    if novo_status not in status_validos:
        return jsonify({"ok": False, "erro": "Status invalido."}), 400

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, assinatura_nome, assinatura_imagem, cliente_id, tecnico_id, movimento_id
        FROM ordem_servico
        WHERE id=? AND tecnico_id=?
    """, (os_id, tecnico_id))
    ordem = cursor.fetchone()
    if not ordem:
        conn.close()
        return jsonify({"ok": False, "erro": "Ordem nao encontrada para este tecnico."}), 404

    status_salvar = novo_status

    if novo_status == "Finalizado":
        tem_assinatura_nome = bool((ordem["assinatura_nome"] or "").strip())
        tem_assinatura_img = bool((ordem["assinatura_imagem"] or "").strip())
        if not tem_assinatura_nome and not tem_assinatura_img:
            conn.close()
            return jsonify({"ok": False, "erro": "Informe a assinatura do cliente antes de finalizar."}), 400

        movimento_id = ordem["movimento_id"]
        if not movimento_id:
            cursor.execute("""
                SELECT COALESCE(SUM(subtotal), 0) AS total
                FROM itens_ordem_servico
                WHERE os_id=?
            """, (os_id,))
            total_os = float(cursor.fetchone()["total"] or 0)

            data_abertura = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("""
                INSERT INTO movimentos (tipo, data_abertura, status, total, cliente_id, funcionario_id)
                VALUES ('OS', ?, 'PENDENTE', ?, ?, ?)
            """, (data_abertura, total_os, ordem["cliente_id"], ordem["tecnico_id"]))
            movimento_id = cursor.lastrowid

            cursor.execute("UPDATE ordem_servico SET movimento_id=? WHERE id=?", (movimento_id, os_id))

        status_salvar = "Aguardando Conferencia"

    cursor.execute("UPDATE ordem_servico SET status=? WHERE id=?", (status_salvar, os_id))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "status_salvo": status_salvar})


@app.route("/api/tecnico/ordem/<int:os_id>/reagendar", methods=["POST"])
def api_tecnico_reagendar_ordem(os_id):
    ensure_ordem_servico_mobile_columns()
    tecnico_id = session.get("user_id")
    if not tecnico_id:
        return jsonify({"ok": False, "erro": "Sessao expirada."}), 401

    dados = request.get_json(silent=True) or {}
    data_agendamento = (dados.get("data_agendamento") or "").strip()
    hora = (dados.get("hora") or "").strip()
    motivo = (dados.get("motivo") or "").strip()

    if len(motivo) < 3:
        return jsonify({"ok": False, "erro": "Informe o motivo do reagendamento."}), 400

    try:
        datetime.strptime(data_agendamento, "%Y-%m-%d")
    except ValueError:
        return jsonify({"ok": False, "erro": "Data de reagendamento invalida."}), 400

    if hora:
        try:
            datetime.strptime(hora, "%H:%M")
        except ValueError:
            return jsonify({"ok": False, "erro": "Hora invalida. Use HH:MM."}), 400

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, status, observacao
        FROM ordem_servico
        WHERE id=? AND tecnico_id=?
        LIMIT 1
    """, (os_id, tecnico_id))
    ordem = cursor.fetchone()
    if not ordem:
        conn.close()
        return jsonify({"ok": False, "erro": "Ordem nao encontrada para este tecnico."}), 404

    if ordem["status"] in ("Finalizado", "Aguardando Conferencia"):
        conn.close()
        return jsonify({"ok": False, "erro": "Nao e possivel reagendar uma OS finalizada/conferida."}), 400

    hora_salvar = hora or (datetime.now().strftime("%H:%M"))
    historico_obs = (ordem["observacao"] or "").strip()
    linha = f"[{datetime.now().strftime('%d/%m/%Y %H:%M')}] Reagendada por falta de peca para {data_agendamento} {hora_salvar}. Motivo: {motivo}"
    observacao_nova = f"{historico_obs}\n{linha}".strip() if historico_obs else linha

    cursor.execute("""
        UPDATE ordem_servico
        SET data_agendamento=?,
            hora=?,
            status='Agendado',
            observacao=?
        WHERE id=?
    """, (data_agendamento, hora_salvar, observacao_nova, os_id))
    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "status_salvo": "Agendado",
        "data_agendamento": data_agendamento,
        "hora": hora_salvar
    })


@app.route("/api/tecnico/ordem/<int:os_id>/evidencias", methods=["POST"])
def api_tecnico_salvar_evidencias(os_id):
    ensure_ordem_servico_mobile_columns()
    tecnico_id = session.get("user_id")
    if not tecnico_id:
        return jsonify({"ok": False, "erro": "Sessao expirada."}), 401

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM ordem_servico WHERE id=? AND tecnico_id=?", (os_id, tecnico_id))
    ordem = cursor.fetchone()
    if not ordem:
        conn.close()
        return jsonify({"ok": False, "erro": "Ordem nao encontrada para este tecnico."}), 404

    assinatura_nome = (request.form.get("assinatura_nome") or "").strip()
    assinatura_imagem = (request.form.get("assinatura_imagem") or "").strip()
    assinatura_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if assinatura_nome else None

    assinatura_img_url = None
    if assinatura_imagem.startswith("data:image/png;base64,"):
        try:
            raw = assinatura_imagem.split(",", 1)[1]
            data = base64.b64decode(raw)
            assinatura_nome_arquivo = f"os_{os_id}_assinatura_{uuid.uuid4().hex[:10]}.png"
            caminho_assinatura = os.path.join(UPLOAD_OS_DIR, assinatura_nome_arquivo)
            with open(caminho_assinatura, "wb") as f:
                f.write(data)
            assinatura_img_url = f"/static/uploads_os/{assinatura_nome_arquivo}"
            assinatura_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, IndexError, base64.binascii.Error):
            assinatura_img_url = None

    campos = ["assinatura_nome=?", "assinatura_data=?"]
    params = [assinatura_nome, assinatura_data]
    if assinatura_img_url:
        campos.append("assinatura_imagem=?")
        params.append(assinatura_img_url)
    params.append(os_id)

    cursor.execute(f"UPDATE ordem_servico SET {', '.join(campos)} WHERE id=?", params)
    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "assinatura_imagem": assinatura_img_url
    })


@app.route("/api/tecnico/ordem/<int:os_id>/adicionar_item", methods=["POST"])
def api_tecnico_adicionar_item(os_id):
    tecnico_id = session.get("user_id")
    if not tecnico_id:
        return jsonify({"ok": False, "erro": "Sessao expirada."}), 401

    data = request.get_json(silent=True) or {}
    try:
        produto_id = int(data.get("produto_id") or 0)
        quantidade = int(data.get("quantidade") or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "erro": "Dados invalidos para produto/quantidade."}), 400

    if produto_id <= 0 or quantidade <= 0:
        return jsonify({"ok": False, "erro": "Informe produto e quantidade validos."}), 400

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, status
        FROM ordem_servico
        WHERE id=? AND tecnico_id=?
        LIMIT 1
    """, (os_id, tecnico_id))
    ordem = cursor.fetchone()
    if not ordem:
        conn.close()
        return jsonify({"ok": False, "erro": "Ordem nao encontrada para este tecnico."}), 404

    if ordem["status"] not in ("Agendado", "Em Atendimento"):
        conn.close()
        return jsonify({"ok": False, "erro": "Nao e possivel adicionar pecas depois de finalizar o atendimento."}), 400

    cursor.execute("""
        SELECT id, nome, preco_venda, estoque, COALESCE(tipo, 'produto') AS tipo
        FROM produtos
        WHERE id=?
        LIMIT 1
    """, (produto_id,))
    produto = cursor.fetchone()
    if not produto:
        conn.close()
        return jsonify({"ok": False, "erro": "Produto nao encontrado."}), 404

    tipo = (produto["tipo"] or "produto").lower()
    estoque_atual = int(produto["estoque"] or 0)
    if tipo != "servico" and estoque_atual < quantidade:
        conn.close()
        return jsonify({"ok": False, "erro": "Estoque insuficiente para essa quantidade."}), 400

    valor = float(produto["preco_venda"] or 0)
    subtotal = valor * quantidade
    cursor.execute("""
        INSERT INTO itens_ordem_servico (os_id, descricao, quantidade, valor, subtotal)
        VALUES (?, ?, ?, ?, ?)
    """, (os_id, produto["nome"], quantidade, valor, subtotal))

    if tipo != "servico":
        cursor.execute("UPDATE produtos SET estoque = estoque - ? WHERE id=?", (quantidade, produto_id))
        estoque_atual -= quantidade

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "item": {
            "descricao": produto["nome"],
            "quantidade": quantidade,
            "valor": valor,
            "subtotal": subtotal
        },
        "estoque_restante": estoque_atual
    })


@app.route("/loja_config", methods=["GET", "POST"])
def loja_config():
    ensure_loja_config_table()
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        acao = request.form.get("acao", "salvar")

        if acao == "ficticia":
            cursor.execute("""
                UPDATE loja_config
                SET nome_fantasia=?,
                    razao_social=?,
                    cnpj=?,
                    telefone=?,
                    endereco=?,
                    numero=?,
                    bairro=?,
                    cidade=?,
                    mensagem_cupom=?
                WHERE id=1
            """, (
                "Central Market Exemplo",
                "Central Market Comercio LTDA",
                "12.345.678/0001-90",
                "(11) 4002-8922",
                "Av. Paulista",
                "1000",
                "Bela Vista",
                "Sao Paulo - SP",
                "Volte sempre! Loja demonstracao."
            ))
        else:
            cursor.execute("""
                UPDATE loja_config
                SET nome_fantasia=?,
                    razao_social=?,
                    cnpj=?,
                    telefone=?,
                    endereco=?,
                    numero=?,
                    bairro=?,
                    cidade=?,
                    mensagem_cupom=?
                WHERE id=1
            """, (
                request.form.get("nome_fantasia"),
                request.form.get("razao_social"),
                request.form.get("cnpj"),
                request.form.get("telefone"),
                request.form.get("endereco"),
                request.form.get("numero"),
                request.form.get("bairro"),
                request.form.get("cidade"),
                request.form.get("mensagem_cupom"),
            ))
        conn.commit()
        conn.close()
        return redirect("/loja_config")

    cursor.execute("SELECT * FROM loja_config WHERE id=1")
    loja = cursor.fetchone()
    editar = request.args.get("editar") == "1"
    conn.close()
    return render_template("loja_config.html", loja=loja, editar=editar)


@app.route("/config_descontos", methods=["GET", "POST"])
def config_descontos():
    ensure_desconto_config_table()
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        def f(nome, default=0):
            try:
                return float(request.form.get(nome, default) or default)
            except (TypeError, ValueError):
                return float(default)

        desconto_livre_percent = max(0.0, f("desconto_livre_percent", 0))
        desconto_livre_valor = max(0.0, f("desconto_livre_valor", 0))
        limite_funcionario_percent = max(0.0, f("limite_funcionario_percent", 5))
        limite_funcionario_valor = max(0.0, f("limite_funcionario_valor", 50))
        limite_gerente_percent = max(0.0, f("limite_gerente_percent", 15))
        limite_gerente_valor = max(0.0, f("limite_gerente_valor", 200))
        limite_proprietario_percent = max(0.0, f("limite_proprietario_percent", 100))
        limite_proprietario_valor = max(0.0, f("limite_proprietario_valor", 999999))

        cursor.execute("""
            UPDATE desconto_config
            SET desconto_livre_percent=?,
                desconto_livre_valor=?,
                limite_funcionario_percent=?,
                limite_funcionario_valor=?,
                limite_gerente_percent=?,
                limite_gerente_valor=?,
                limite_proprietario_percent=?,
                limite_proprietario_valor=?
            WHERE id=1
        """, (
            desconto_livre_percent,
            desconto_livre_valor,
            limite_funcionario_percent,
            limite_funcionario_valor,
            limite_gerente_percent,
            limite_gerente_valor,
            limite_proprietario_percent,
            limite_proprietario_valor
        ))
        conn.commit()
        conn.close()
        return redirect("/config_descontos")

    cursor.execute("SELECT * FROM desconto_config WHERE id=1")
    cfg = cursor.fetchone()
    conn.close()
    return render_template("config_descontos.html", cfg=cfg)
#====================================f=============
#rota Categories
#=================================================
@app.route("/categorias", methods=["GET", "POST"])
def categorias():
    conn = get_db()
    cursor = conn.cursor()

    # SALVAR NOVA CATEGORIA
    if request.method == "POST":
        nome = request.form["nome"]
        categoria_pai_id = request.form.get("categoria_pai_id")

        cursor.execute(
            "INSERT INTO categorias (nome, categoria_pai_id) VALUES (?, ?)",
            (nome, categoria_pai_id if categoria_pai_id else None)
        )
        conn.commit()

    # BUSCA
    busca = request.args.get("busca", "").strip()

    if busca:
        cursor.execute(
            "SELECT * FROM categorias WHERE nome LIKE ?",
            ("%" + busca + "%",)
        )
    else:
        cursor.execute("SELECT * FROM categorias")

    categorias = cursor.fetchall()
    conn.close()

    return render_template("categorias.html", categorias=categorias)
#=================================================
#EDITAR CATEGORIA
#=================================================
@app.route("/editar_categoria/<int:id>", methods=["GET", "POST"])
def editar_categoria(id):
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        nome = request.form["nome"]

        cursor.execute("UPDATE categorias SET nome = ? WHERE id = ?", (nome, id))
        conn.commit()
        conn.close()
        return redirect("/categorias")

    cursor.execute("SELECT * FROM categorias WHERE id = ?", (id,))
    categoria = cursor.fetchone()
    conn.close()

    return render_template("editar_categoria.html", categoria=categoria)
#=================================================
#rota fornecedores
#=================================================
@app.route("/fornecedores", methods=["GET", "POST"])
def fornecedores():
    conn = get_db()
    cursor = conn.cursor()

    # =========================
    # CADASTRAR FORNECEDOR
    # =========================
    if request.method == "POST":

        razao_social = request.form.get("razao_social")
        nome_fantasia = request.form.get("nome_fantasia")
        cnpj = request.form.get("cnpj")
        telefone = request.form.get("telefone")
        email = request.form.get("email")
        vendedor = request.form.get("vendedor")
        telefone_vendedor = request.form.get("telefone_vendedor")

        # novos campos
        site = request.form.get("site")
        usuario_site = request.form.get("usuario_site")
        senha_site = request.form.get("senha_site")
        observacoes = request.form.get("observacoes")

        cursor.execute("""
            INSERT INTO fornecedores (
                razao_social,
                nome_fantasia,
                cnpj,
                telefone,
                email,
                vendedor,
                telefone_vendedor,
                site,
                usuario_site,
                senha_site,
                observacoes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            razao_social,
            nome_fantasia,
            cnpj,
            telefone,
            email,
            vendedor,
            telefone_vendedor,
            site,
            usuario_site,
            senha_site,
            observacoes
        ))

        conn.commit()

    # =========================
    # BUSCA INTELIGENTE
    # =========================
    busca = request.args.get("busca", "").strip()

    if busca:
        cursor.execute("""
            SELECT * FROM fornecedores
            WHERE razao_social LIKE ?
            OR nome_fantasia LIKE ?
            OR cnpj LIKE ?
            OR vendedor LIKE ?
            OR telefone LIKE ?
        """, (
            f"%{busca}%",
            f"%{busca}%",
            f"%{busca}%",
            f"%{busca}%",
            f"%{busca}%"
        ))
    else:
        cursor.execute("SELECT * FROM fornecedores ORDER BY id DESC")

    fornecedores = cursor.fetchall()

    conn.close()

    return render_template(
        "fornecedores.html",
        fornecedores=fornecedores
    )
#=================================================
#EDITOR FORNECEDORES^^^^^^^^^^
#=================================================
@app.route("/editar_fornecedor/<int:id>", methods=["GET", "POST"])
def editar_fornecedor(id):
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        cursor.execute("""
            UPDATE fornecedores SET
                razao_social=?,
                nome_fantasia=?,
                cnpj=?,
                telefone=?,
                email=?,
                vendedor=?,
                telefone_vendedor=?,
                site=?,
                usuario_site=?,
                senha_site=?,
                observacoes=?
            WHERE id=?
        """, (
            request.form.get("razao_social"),
            request.form.get("nome_fantasia"),
            request.form.get("cnpj"),
            request.form.get("telefone"),
            request.form.get("email"),
            request.form.get("vendedor"),
            request.form.get("telefone_vendedor"),
            request.form.get("site"),
            request.form.get("usuario_site"),
            request.form.get("senha_site"),
            request.form.get("observacoes"),
            id
        ))

        conn.commit()
        conn.close()
        return redirect("/fornecedores")

    cursor.execute("SELECT * FROM fornecedores WHERE id=?", (id,))
    fornecedor = cursor.fetchone()
    conn.close()

    return render_template("editar_fornecedor.html", fornecedor=fornecedor)
#=================================================
#rota botão excluir fornecedor
#=================================================
@app.route("/excluir_fornecedor/<int:id>")
def excluir_fornecedor(id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fornecedores WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect("/fornecedores")
#=================================================
#rota produtos^^^^^^^^^^
#=================================================
@app.route("/produtos", methods=["GET", "POST"])
def produtos():

    conn = conectar()
    cursor = conn.cursor()

    erro_map = {
        "campos_obrigatorios": "Preencha SKU, nome, categoria, fornecedor, preco de custo e preco de venda.",
        "preco_invalido": "Informe valores validos para preco de custo e preco de venda.",
        "estoque_minimo_invalido": "Estoque minimo deve ser um numero inteiro maior ou igual a zero.",
        "tipo_invalido": "Tipo de item invalido.",
        "sku_duplicado": "Ja existe um produto com este SKU.",
        "codigo_barras_duplicado": "Ja existe um produto com este codigo de barras.",
        "erro_interno": "Nao foi possivel salvar o produto agora."
    }

    # ---------------- CADASTRO ----------------
    if request.method == "POST":
        sku = (request.form.get("sku") or "").strip()
        nome = (request.form.get("nome") or "").strip()
        codigo_fabricante = (request.form.get("codigo_fabricante") or "").strip()
        codigo_barras = (request.form.get("codigo_barras") or "").strip()
        categoria_id = (request.form.get("categoria_id") or "").strip()
        fornecedor_id = (request.form.get("fornecedor_id") or "").strip()
        unidade = (request.form.get("unidade") or "").strip()
        ncm = (request.form.get("ncm") or "").strip()
        tipo = (request.form.get("tipo") or "").strip().lower()

        if not sku or not nome or not categoria_id or not fornecedor_id:
            conn.close()
            return redirect("/produtos?erro=campos_obrigatorios")

        try:
            preco_custo = float(request.form.get("preco_custo") or 0)
            preco_venda = float(request.form.get("preco_venda") or 0)
        except (TypeError, ValueError):
            conn.close()
            return redirect("/produtos?erro=preco_invalido")

        if preco_custo < 0 or preco_venda < 0:
            conn.close()
            return redirect("/produtos?erro=preco_invalido")

        try:
            estoque_minimo = int(request.form.get("estoque_minimo") or 0)
        except (TypeError, ValueError):
            conn.close()
            return redirect("/produtos?erro=estoque_minimo_invalido")

        if estoque_minimo < 0:
            conn.close()
            return redirect("/produtos?erro=estoque_minimo_invalido")

        if tipo not in ("produto", "servico"):
            conn.close()
            return redirect("/produtos?erro=tipo_invalido")

        cursor.execute("SELECT id FROM produtos WHERE sku = ? LIMIT 1", (sku,))
        if cursor.fetchone():
            conn.close()
            return redirect("/produtos?erro=sku_duplicado")

        if codigo_barras:
            cursor.execute("SELECT id FROM produtos WHERE codigo_barras = ? LIMIT 1", (codigo_barras,))
            if cursor.fetchone():
                conn.close()
                return redirect("/produtos?erro=codigo_barras_duplicado")

        # 🔥 Se for serviço, não controla estoque
        if tipo == "servico":
            estoque = 0
            estoque_minimo = 0
        else:
            estoque = 0  # começa zerado até entrada manual depois

        try:
            cursor.execute("""
                INSERT INTO produtos
                (sku, nome, codigo_barras, codigo_fabricante,
                 categoria_id, preco_custo, preco_venda,
                 unidade, estoque, estoque_minimo, ncm, fornecedor_id, tipo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sku, nome, codigo_barras, codigo_fabricante,
                categoria_id, preco_custo, preco_venda,
                unidade, estoque, estoque_minimo, ncm, fornecedor_id, tipo
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            conn.close()
            return redirect("/produtos?erro=sku_duplicado")
        except Exception:
            conn.close()
            return redirect("/produtos?erro=erro_interno")

        conn.close()
        return redirect("/produtos?ok=1")

    # ---------------- BUSCA INTELIGENTE ----------------
    busca = request.args.get("busca", "").strip()
    erro = erro_map.get(request.args.get("erro", "").strip(), "")
    ok = request.args.get("ok", "").strip() == "1"

    if busca:
        cursor.execute("""
            SELECT p.*,
                   c.nome AS categoria_nome,
                   f.razao_social AS fornecedor_nome
            FROM produtos p
            LEFT JOIN categorias c ON c.id = p.categoria_id
            LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
            WHERE nome LIKE ?
            OR sku LIKE ?
            OR codigo_barras LIKE ?
            OR codigo_fabricante LIKE ?
            ORDER BY p.id DESC
        """, (
            f"%{busca}%",
            f"%{busca}%",
            f"%{busca}%",
            f"%{busca}%"
        ))
    else:
        cursor.execute("""
            SELECT p.*,
                   c.nome AS categoria_nome,
                   f.razao_social AS fornecedor_nome
            FROM produtos p
            LEFT JOIN categorias c ON c.id = p.categoria_id
            LEFT JOIN fornecedores f ON f.id = p.fornecedor_id
            ORDER BY p.id DESC
        """)

    produtos = cursor.fetchall()

    # ---------------- LISTAS AUXILIARES ----------------
    cursor.execute("SELECT * FROM categorias")
    categorias = cursor.fetchall()

    cursor.execute("SELECT * FROM fornecedores")
    fornecedores = cursor.fetchall()

    conn.close()

    return render_template(
        "produtos.html",
        categorias=categorias,
        fornecedores=fornecedores,
        produtos=produtos,
        busca=busca,
        erro=erro,
        ok=ok
    )
#=================================================
#editar produtos^^^^^^^^^^^
#=================================================
@app.route("/editar/<int:id>", methods=["GET", "POST"])
def editar(id):

    conn = conectar()
    cursor = conn.cursor()

    if request.method == "POST":
        sku = request.form["sku"]
        nome = request.form["nome"]
        codigo_fabricante = request.form["codigo_fabricante"]
        codigo_barras = request.form["codigo_barras"]
        categoria_id = request.form["categoria_id"]
        fornecedor_id = request.form["fornecedor_id"]
        preco_custo = request.form["preco_custo"]
        preco_venda = request.form["preco_venda"]
        unidade = request.form["unidade"]
        estoque = request.form["estoque"]
        estoque_minimo = request.form["estoque_minimo"]
        ncm = request.form["ncm"]
        tipo = request.form["tipo"]   # 🔥 NOVO CAMPO

        # 🔥 Se for serviço, zera estoque
        if tipo == "servico":
            estoque = 0
            estoque_minimo = 0

        cursor.execute("""
            UPDATE produtos
            SET sku = ?, nome = ?, codigo_fabricante = ?, codigo_barras = ?,
                categoria_id = ?, fornecedor_id = ?, preco_custo = ?,
                preco_venda = ?, unidade = ?, estoque = ?, 
                estoque_minimo = ?, ncm = ?, tipo = ?
            WHERE id = ?
        """, (
            sku, nome, codigo_fabricante, codigo_barras,
            categoria_id, fornecedor_id, preco_custo,
            preco_venda, unidade, estoque,
            estoque_minimo, ncm, tipo, id
        ))

        conn.commit()
        conn.close()

        return redirect("/produtos")

    cursor.execute("SELECT * FROM produtos WHERE id = ?", (id,))
    produto = cursor.fetchone()

    cursor.execute("SELECT * FROM categorias")
    categorias = cursor.fetchall()

    cursor.execute("SELECT * FROM fornecedores")
    fornecedores = cursor.fetchall()

    conn.close()

    return render_template(
        "editar_produto.html",
        produto=produto,
        categorias=categorias,
        fornecedores=fornecedores
    )
#=================================================
#começamos o pdv com ordem de serviços
# rota novo movimento
#=================================================
@app.route("/novo_movimento/<tipo>")
def novo_movimento(tipo):
    conn = get_db()
    cursor = conn.cursor()

    data_abertura = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO movimentos (tipo, data_abertura, status, total)
        VALUES (?, ?, ?, 0)
    """, (tipo, data_abertura, "PENDENTE"))

    movimento_id = cursor.lastrowid

    conn.commit()
    conn.close()

    return redirect(f"/movimento/{movimento_id}")
#=================================================
#Criar a rota movimento
#=================================================
@app.route("/movimento/<int:id>")
def movimento(id):
    conn = get_db()
    cursor = conn.cursor()

    # 🔎 Buscar dados do movimento
    cursor.execute("""
        SELECT id, tipo, cliente_id, funcionario_id,
               data_abertura, total, status
        FROM movimentos
        WHERE id = ?
    """, (id,))
    
    mov = cursor.fetchone()

    if not mov:
        conn.close()
        return "Movimento não encontrado"

    # 🔎 Buscar itens com JOIN para pegar nome do produto
    cursor.execute("""
        SELECT im.id,
               p.nome,
               im.quantidade,
               im.preco_unitario,
               im.subtotal
        FROM itens_movimento im
        JOIN produtos p ON im.produto_id = p.id
        WHERE im.movimento_id = ?
    """, (id,))

    itens = cursor.fetchall()

    conn.close()

    return render_template("movimento.html", mov=mov, itens=itens)
#=================================================
#Criar rota para adicionar item
#=================================================
@app.route("/add_item/<int:movimento_id>", methods=["POST"])
def add_item(movimento_id):
    conn = get_db()
    cursor = conn.cursor()

    # 🔒 Verifica se está finalizada
    cursor.execute("SELECT status FROM movimentos WHERE id = ?", (movimento_id,))
    status = cursor.fetchone()[0]

    if status == "FINALIZADO":
        conn.close()
        return "Venda já finalizada!"

    busca = request.form["busca"]
    quantidade = int(request.form["quantidade"])

    # 🔎 Busca produto
    cursor.execute("""
        SELECT id, nome, preco_venda, estoque
        FROM produtos
        WHERE nome LIKE ?
        OR codigo_barras LIKE ?
        OR codigo_fabricante LIKE ?
        OR sku LIKE ?
        LIMIT 1
    """, (f"%{busca}%", f"%{busca}%", f"%{busca}%", f"%{busca}%"))

    produto = cursor.fetchone()

    if not produto:
        conn.close()
        return "Produto não encontrado!"

    produto_id = produto[0]
    nome = produto[1]
    preco = produto[2]
    estoque = produto[3]

    if estoque < quantidade:
        conn.close()
        return "Estoque insuficiente!"

    subtotal = preco * quantidade

    # 📦 Inserir item
    cursor.execute("""
        INSERT INTO itens_movimento
        (movimento_id, produto_id, quantidade, preco_unitario, subtotal)
        VALUES (?, ?, ?, ?, ?)
    """, (movimento_id, produto_id, quantidade, preco, subtotal))

    # 💰 Atualizar total
    cursor.execute("""
        UPDATE movimentos
        SET total = COALESCE(total, 0) + ?
        WHERE id = ?
    """, (subtotal, movimento_id))

    # 📉 Baixar estoque
    cursor.execute("""
        UPDATE produtos
        SET estoque = estoque - ?
        WHERE id = ?
    """, (quantidade, produto_id))

    conn.commit()
    conn.close()

    return redirect(f"/movimento/{movimento_id}")
#=================================================
#Criar rota remover_item
#=================================================
@app.route("/remover_item/<int:item_id>", methods=["POST"])
def remover_item(item_id):
    conn = get_db()
    cursor = conn.cursor()

    # Buscar item
    cursor.execute("""
        SELECT movimento_id, produto_id, quantidade, subtotal
        FROM itens_movimento
        WHERE id = ?
    """, (item_id,))

    item = cursor.fetchone()

    if not item:
        conn.close()
        return "Item não encontrado"

    movimento_id = item[0]
    produto_id = item[1]
    quantidade = item[2]
    subtotal = item[3]

    # 🔺 Devolver estoque
    cursor.execute("""
        UPDATE produtos
        SET estoque = estoque + ?
        WHERE id = ?
    """, (quantidade, produto_id))

    # 🔻 Atualizar total da venda
    cursor.execute("""
        UPDATE movimentos
        SET total = total - ?
        WHERE id = ?
    """, (subtotal, movimento_id))

    # 🗑 Apagar item
    cursor.execute("""
        DELETE FROM itens_movimento
        WHERE id = ?
    """, (item_id,))

    conn.commit()
    conn.close()

    return redirect(f"/movimento/{movimento_id}")
# Rota do PDV
@app.route("/pdv")
def pdv():
    from flask import redirect, render_template, session

    conn = get_db()
    cursor = conn.cursor()

    # 🔎 Verifica se existe caixa aberto
    cursor.execute("SELECT id FROM caixas WHERE status='ABERTO' LIMIT 1")
    caixa = cursor.fetchone()

    if not caixa:
        return redirect("/caixa")

    caixa_id = caixa[0]

    # 📦 Buscar produtos para o PDV
    cursor.execute("""
        SELECT id, nome, preco_venda, estoque
        FROM produtos
        WHERE estoque > 0
    """)
    produtos = cursor.fetchall()

    # 🛒 Carrinho da sessão
    carrinho = session.get("carrinho", [])

    return render_template(
        "pdv.html",
        produtos=produtos,
        carrinho=carrinho,
        caixa_id=caixa_id
    )
#=================================================
#rota finalizar venda
#=================================================
# Rota para finalizar venda
@app.route("/finalizar_venda", methods=["POST"])
def finalizar_venda():

    if not request.is_json:
        return jsonify({"erro": "Formato inválido"}), 415

    dados = request.get_json()

    conn = get_db()
    cursor = conn.cursor()

    itens = dados.get("itens", [])
    forma = dados.get("forma_pagamento", "").upper()

    if not itens:
        return jsonify({"erro": "Carrinho vazio"})

    # buscar caixa aberto
    cursor.execute("SELECT id FROM caixas WHERE status='ABERTO' LIMIT 1")
    caixa = cursor.fetchone()

    if not caixa:
        return jsonify({"erro": "Nenhum caixa aberto"})

    caixa_id = caixa[0]

    total = sum(i["preco"] * i["quantidade"] for i in itens)

    cursor.execute("""
        INSERT INTO movimentos
        (tipo, data_abertura, total, status, caixa_id, forma_pagamento)
        VALUES ('VENDA', datetime('now'), ?, 'FECHADA', ?, ?)
    """, (total, caixa_id, forma))

    movimento_id = cursor.lastrowid

    for item in itens:
        cursor.execute("""
            INSERT INTO itens_movimento
            (movimento_id, produto_id, quantidade, preco_unitario, subtotal)
            VALUES (?, ?, ?, ?, ?)
        """, (
            movimento_id,
            item["id"],
            item["quantidade"],
            item["preco"],
            item["preco"] * item["quantidade"]
        ))

        cursor.execute("""
            UPDATE produtos
            SET estoque = estoque - ?
            WHERE id = ?
        """, (item["quantidade"], item["id"]))

    conn.commit()
    conn.close()

    return jsonify({"sucesso": True})
#=================================================
#Preparar a rota de busca no backend
#=================================================

# Rota de busca de produtos (autocomplete)
@app.route("/buscar_produto")
def buscar_produto():
    termo = request.args.get("q", "").strip()

    conn = get_db()
    cursor = conn.cursor()

    like = f"%{termo}%"

    cursor.execute("""
        SELECT id, nome, preco_venda AS preco, estoque, tipo, sku, codigo_barras, codigo_fabricante
        FROM produtos
        WHERE
            nome LIKE ?
            OR sku LIKE ?
            OR codigo_barras LIKE ?
            OR codigo_fabricante LIKE ?
            OR substr(codigo_barras, -6) = ?
        
        UNION ALL
        
        SELECT id, nome, preco AS preco, 999 AS estoque, 'servico' AS tipo, '' AS sku, '' AS codigo_barras, '' AS codigo_fabricante
        FROM servicos
        WHERE nome LIKE ?
        
        LIMIT 10
    """, (
        like, like, like, like, termo,
        like
    ))

    resultados = cursor.fetchall()
    conn.close()

    lista = []
    for r in resultados:
        lista.append({
            "id": r[0],
            "nome": r[1],
            "preco": r[2],
            "estoque": r[3],
            "tipo": r[4],
            "sku": r[5] or "",
            "codigo_barras": r[6] or "",
            "codigo_fabricante": r[7] or ""
        })

    return jsonify(lista)
#=================================================
#Rota para abrir caixa
#=================================================
@app.route("/abrir_caixa", methods=["POST"])
def abrir_caixa():
    from datetime import datetime
    from flask import request, redirect

    conn = get_db()
    cursor = conn.cursor()

    # 🔎 Verifica se já existe caixa aberto
    cursor.execute("SELECT id FROM caixas WHERE status='ABERTO' LIMIT 1")
    if cursor.fetchone():
        return "Já existe um caixa aberto!", 400

    # Pega valor inicial
    try:
        valor_inicial = float(request.form.get("valor_inicial"))
    except:
        return "Valor inválido!", 400

    data_abertura = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO caixas (data_abertura, valor_inicial, status)
        VALUES (?, ?, ?)
    """, (data_abertura, valor_inicial, "ABERTO"))

    conn.commit()

    return redirect("/caixa")
#=================================================
#Rota para fechar caixa
#=================================================
@app.route("/fechar_caixa", methods=["GET", "POST"])
def fechar_caixa():
    from datetime import datetime
    if request.method == "GET":
        return redirect("/caixa")

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT id, valor_inicial FROM caixas WHERE status='ABERTO' LIMIT 1")
    caixa = cursor.fetchone()

    if not caixa:
        conn.close()
        return "Nenhum caixa aberto"

    caixa_id = caixa[0]
    valor_inicial = caixa[1]

    # totais por forma de pagamento
    cursor.execute("""
        SELECT forma_pagamento, SUM(total)
        FROM movimentos
        WHERE caixa_id=? AND status='PAGO'
        GROUP BY forma_pagamento
    """, (caixa_id,))

    totais = cursor.fetchall()

    formas_pagamento = {}
    for forma, valor in totais:
        chave = forma or "NAO INFORMADO"
        formas_pagamento[chave] = float(valor or 0)

    dinheiro = formas_pagamento.get("DINHEIRO", 0)
    pix = formas_pagamento.get("PIX", 0)
    cartao = formas_pagamento.get("CARTAO", 0)
    total_vendas = float(sum(formas_pagamento.values()))
    valor_final = valor_inicial + total_vendas
    ticket_medio = (total_vendas / len(totais)) if totais else 0

    cursor.execute("""
        SELECT COUNT(*)
        FROM movimentos
        WHERE caixa_id=? AND status='PAGO'
    """, (caixa_id,))
    qtd_vendas = cursor.fetchone()[0] or 0
    if qtd_vendas:
        ticket_medio = total_vendas / qtd_vendas

    data_fechamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        UPDATE caixas
        SET status='FECHADO',
            data_fechamento=?,
            valor_final=?
        WHERE id=?
    """, (data_fechamento, valor_final, caixa_id))

    conn.commit()
    conn.close()

    return render_template(
        "fechamento_caixa.html",
        caixa_id=caixa_id,
        data_fechamento=data_fechamento,
        valor_inicial=float(valor_inicial or 0),
        dinheiro=float(dinheiro or 0),
        pix=float(pix or 0),
        cartao=float(cartao or 0),
        total_vendas=float(total_vendas or 0),
        qtd_vendas=int(qtd_vendas or 0),
        ticket_medio=float(ticket_medio or 0),
        valor_final=float(valor_final or 0),
        formas_pagamento=formas_pagamento
    )


@app.route("/imprimir_fechamento_caixa/<int:caixa_id>")
def imprimir_fechamento_caixa(caixa_id):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, data_abertura, data_fechamento, valor_inicial, valor_final, status
        FROM caixas
        WHERE id=?
        LIMIT 1
    """, (caixa_id,))
    caixa = cursor.fetchone()

    if not caixa:
        conn.close()
        return "Caixa nao encontrado.", 404

    cursor.execute("""
        SELECT COALESCE(forma_pagamento, 'NAO INFORMADO') AS forma, COALESCE(SUM(total), 0) AS total
        FROM movimentos
        WHERE caixa_id=? AND status='PAGO'
        GROUP BY forma_pagamento
    """, (caixa_id,))
    totais = cursor.fetchall()

    formas_pagamento = {t["forma"]: float(t["total"] or 0) for t in totais}
    dinheiro = formas_pagamento.get("DINHEIRO", 0.0)
    pix = formas_pagamento.get("PIX", 0.0)
    cartao = formas_pagamento.get("CARTAO", 0.0)
    total_vendas = float(sum(formas_pagamento.values()))

    cursor.execute("""
        SELECT COUNT(*)
        FROM movimentos
        WHERE caixa_id=? AND status='PAGO'
    """, (caixa_id,))
    qtd_vendas = cursor.fetchone()[0] or 0
    ticket_medio = (total_vendas / qtd_vendas) if qtd_vendas else 0.0

    conn.close()

    return render_template(
        "fechamento_caixa_cupom.html",
        caixa_id=caixa["id"],
        data_abertura=caixa["data_abertura"],
        data_fechamento=caixa["data_fechamento"],
        status=caixa["status"],
        valor_inicial=float(caixa["valor_inicial"] or 0),
        valor_final=float(caixa["valor_final"] or 0),
        total_vendas=float(total_vendas or 0),
        qtd_vendas=int(qtd_vendas or 0),
        ticket_medio=float(ticket_medio or 0),
        dinheiro=float(dinheiro or 0),
        pix=float(pix or 0),
        cartao=float(cartao or 0),
        formas_pagamento=formas_pagamento
    )
#=================================================
#TELA PARA ABRIR CAIXA
#=================================================
@app.route("/caixa")
def tela_caixa():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM caixas WHERE status='ABERTO' LIMIT 1")
    caixa = cursor.fetchone()

    total_vendas = 0
    qtd_vendas = 0
    ticket_medio = 0
    formas_pagamento = {}
    vendas_por_hora = []
    ultimas_vendas = []
    pendentes_qtd = 0
    pendentes_total = 0
    ultimo_fechamento = None

    if caixa:
        caixa_id = caixa["id"]

        cursor.execute("""
            SELECT SUM(total), COUNT(*)
            FROM movimentos
            WHERE caixa_id=? AND status='PAGO'
        """, (caixa_id,))

        resultado = cursor.fetchone()

        if resultado:
            total_vendas = resultado[0] or 0
            qtd_vendas = resultado[1] or 0
            ticket_medio = (total_vendas / qtd_vendas) if qtd_vendas else 0

        cursor.execute("""
            SELECT COALESCE(forma_pagamento, 'NAO INFORMADO') AS forma,
                   COALESCE(SUM(total), 0) AS total
            FROM movimentos
            WHERE caixa_id=? AND status='PAGO'
            GROUP BY forma_pagamento
        """, (caixa_id,))
        formas = cursor.fetchall()
        formas_pagamento = {f["forma"]: float(f["total"] or 0) for f in formas}

        cursor.execute("""
            SELECT substr(data_abertura, 12, 2) AS hora,
                   COALESCE(SUM(total), 0) AS total
            FROM movimentos
            WHERE caixa_id=? AND status='PAGO'
            GROUP BY substr(data_abertura, 12, 2)
            ORDER BY hora
        """, (caixa_id,))
        horas = cursor.fetchall()
        vendas_por_hora = [{"hora": h["hora"], "total": float(h["total"] or 0)} for h in horas]

        cursor.execute("""
            SELECT id, total, forma_pagamento, data_abertura
            FROM movimentos
            WHERE caixa_id=? AND status='PAGO'
            ORDER BY id DESC
            LIMIT 8
        """, (caixa_id,))
        vendas = cursor.fetchall()
        ultimas_vendas = [
            {
                "id": v["id"],
                "total": float(v["total"] or 0),
                "forma": v["forma_pagamento"] or "NAO INFORMADO",
                "data": v["data_abertura"] or ""
            }
            for v in vendas
        ]

    cursor.execute("""
        SELECT COUNT(*), COALESCE(SUM(total), 0)
        FROM movimentos
        WHERE status='PENDENTE'
    """)
    pendentes = cursor.fetchone()
    if pendentes:
        pendentes_qtd = pendentes[0] or 0
        pendentes_total = float(pendentes[1] or 0)

    cursor.execute("""
        SELECT id, data_abertura, data_fechamento, valor_inicial, valor_final
        FROM caixas
        WHERE status='FECHADO'
        ORDER BY id DESC
        LIMIT 1
    """)
    ultimo = cursor.fetchone()
    if ultimo:
        ultimo_fechamento = {
            "id": ultimo["id"],
            "data_abertura": ultimo["data_abertura"],
            "data_fechamento": ultimo["data_fechamento"],
            "valor_inicial": float(ultimo["valor_inicial"] or 0),
            "valor_final": float(ultimo["valor_final"] or 0),
        }

    conn.close()

    return render_template(
        "caixa.html",
        caixa=caixa,
        total_vendas=total_vendas,
        qtd_vendas=qtd_vendas,
        ticket_medio=ticket_medio,
        formas_pagamento=formas_pagamento,
        vendas_por_hora=vendas_por_hora,
        ultimas_vendas=ultimas_vendas,
        pendentes_qtd=pendentes_qtd,
        pendentes_total=pendentes_total,
        ultimo_fechamento=ultimo_fechamento
    )

#=================================================
#total vendido
#=================================================
@app.route("/total_caixa")
def total_caixa():
    conn = get_db()
    cursor = conn.cursor()

    # verifica caixa aberto
    cursor.execute("SELECT id FROM caixas WHERE status='ABERTO' LIMIT 1")
    caixa = cursor.fetchone()

    if not caixa:
        return {"total": 0, "vendas": 0}

    caixa_id = caixa[0]

    cursor.execute("""
        SELECT 
            COUNT(*),
            COALESCE(SUM(total),0)
        FROM movimentos
        WHERE caixa_id=? AND status='FECHADA'
    """, (caixa_id,))

    vendas, total = cursor.fetchone()

    return {"total": total, "vendas": vendas}
#=================================================
#painel_caixa
#=================================================
@app.route("/painel_caixa")
def painel_caixa():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, valor_inicial, data_abertura
        FROM caixas
        WHERE status='ABERTO'
        LIMIT 1
    """)
    caixa = cursor.fetchone()

    if not caixa:
        return "<h2>Nenhum caixa aberto</h2>"

    caixa_id, valor_inicial, data_abertura = caixa

    cursor.execute("""
        SELECT COUNT(*), COALESCE(SUM(total),0)
        FROM movimentos
        WHERE caixa_id=? AND status='FECHADA'
    """, (caixa_id,))

    vendas, total = cursor.fetchone()

    return render_template(
        "painel_caixa.html",
        total=total,
        vendas=vendas,
        valor_inicial=valor_inicial,
        data_abertura=data_abertura
    )


@app.route("/financeiro", methods=["GET", "POST"])
def financeiro():
    ensure_financeiro_table()
    ensure_contas_financeiras_table()
    ensure_financeiro_metas_table()
    ensure_movimentos_desconto_columns()

    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        tipo = (request.form.get("tipo") or "").strip().upper()
        categoria = (request.form.get("categoria") or "").strip()
        descricao = (request.form.get("descricao") or "").strip()
        data_lancamento = (request.form.get("data_lancamento") or "").strip()

        valor = parse_float_br(request.form.get("valor"), 0)

        if tipo not in ("ENTRADA", "DESPESA"):
            conn.close()
            return "Tipo invalido.", 400
        if valor <= 0:
            conn.close()
            return "Valor deve ser maior que zero.", 400

        try:
            datetime.strptime(data_lancamento, "%Y-%m-%d")
        except ValueError:
            conn.close()
            return "Data invalida.", 400

        cursor.execute("SELECT id FROM caixas WHERE status='ABERTO' LIMIT 1")
        caixa = cursor.fetchone()
        caixa_id = caixa["id"] if caixa else None

        cursor.execute("""
            INSERT INTO financeiro_lancamentos
            (tipo, categoria, descricao, valor, data_lancamento, caixa_id, funcionario_id, origem)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'MANUAL')
        """, (
            tipo,
            categoria,
            descricao,
            valor,
            data_lancamento,
            caixa_id,
            session.get("user_id"),
        ))
        conn.commit()
        conn.close()
        return redirect("/financeiro")

    hoje = datetime.now().date()
    inicio_padrao = hoje.replace(day=1).strftime("%Y-%m-%d")
    fim_padrao = hoje.strftime("%Y-%m-%d")

    data_inicio = (request.args.get("inicio") or inicio_padrao).strip()
    data_fim = (request.args.get("fim") or fim_padrao).strip()

    try:
        dt_inicio = datetime.strptime(data_inicio, "%Y-%m-%d").date()
        dt_fim = datetime.strptime(data_fim, "%Y-%m-%d").date()
    except ValueError:
        dt_inicio = hoje.replace(day=1)
        dt_fim = hoje
        data_inicio = dt_inicio.strftime("%Y-%m-%d")
        data_fim = dt_fim.strftime("%Y-%m-%d")

    if dt_inicio > dt_fim:
        dt_inicio, dt_fim = dt_fim, dt_inicio
        data_inicio = dt_inicio.strftime("%Y-%m-%d")
        data_fim = dt_fim.strftime("%Y-%m-%d")

    def _fim_mes(dt):
        return (dt.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    def _dre_periodo(inicio_dt, fim_dt):
        periodo = (inicio_dt.strftime("%Y-%m-%d"), fim_dt.strftime("%Y-%m-%d"))

        cursor.execute("""
            SELECT
                COUNT(*) AS qtd_vendas,
                COALESCE(SUM(total), 0) AS receita_liquida,
                COALESCE(SUM(COALESCE(desconto_valor, 0)), 0) AS descontos
            FROM movimentos
            WHERE status='PAGO'
              AND substr(data_abertura, 1, 10) BETWEEN ? AND ?
        """, periodo)
        vendas = cursor.fetchone()
        qtd_vendas = int(vendas["qtd_vendas"] or 0)
        receita_liquida = float(vendas["receita_liquida"] or 0)
        descontos = float(vendas["descontos"] or 0)
        receita_bruta = receita_liquida + descontos

        cursor.execute("""
            SELECT
                COALESCE(SUM(im.quantidade * COALESCE(p.preco_custo, 0)), 0) AS cmv
            FROM itens_movimento im
            JOIN movimentos m ON m.id = im.movimento_id
            LEFT JOIN produtos p ON p.id = im.produto_id
            WHERE m.status='PAGO'
              AND substr(m.data_abertura, 1, 10) BETWEEN ? AND ?
        """, periodo)
        cmv = float(cursor.fetchone()["cmv"] or 0)

        cursor.execute("""
            SELECT COALESCE(SUM(valor), 0) AS total
            FROM financeiro_lancamentos
            WHERE tipo='ENTRADA' AND data_lancamento BETWEEN ? AND ?
        """, periodo)
        entradas_manuais = float(cursor.fetchone()["total"] or 0)

        cursor.execute("""
            SELECT COALESCE(SUM(valor), 0) AS total
            FROM financeiro_lancamentos
            WHERE tipo='DESPESA' AND data_lancamento BETWEEN ? AND ?
        """, periodo)
        despesas_manuais = float(cursor.fetchone()["total"] or 0)

        cursor.execute("""
            SELECT COALESCE(SUM(valor), 0) AS total
            FROM contas_financeiras
            WHERE tipo='RECEBER'
              AND status='PAGO'
              AND data_pagamento BETWEEN ? AND ?
        """, periodo)
        contas_recebidas = float(cursor.fetchone()["total"] or 0)

        cursor.execute("""
            SELECT COALESCE(SUM(valor), 0) AS total
            FROM contas_financeiras
            WHERE tipo='PAGAR'
              AND status='PAGO'
              AND data_pagamento BETWEEN ? AND ?
        """, periodo)
        contas_pagas = float(cursor.fetchone()["total"] or 0)

        outras_receitas = entradas_manuais + contas_recebidas
        despesas_operacionais = despesas_manuais + contas_pagas
        lucro_bruto = receita_liquida - cmv
        resultado = lucro_bruto + outras_receitas - despesas_operacionais
        margem = (resultado / receita_liquida * 100.0) if receita_liquida > 0 else 0.0
        ticket_medio = (receita_liquida / qtd_vendas) if qtd_vendas else 0.0

        return {
            "receita_bruta": receita_bruta,
            "descontos": descontos,
            "receita_liquida": receita_liquida,
            "cmv": cmv,
            "lucro_bruto": lucro_bruto,
            "outras_receitas": outras_receitas,
            "despesas_operacionais": despesas_operacionais,
            "resultado": resultado,
            "margem": margem,
            "qtd_vendas": qtd_vendas,
            "ticket_medio": ticket_medio,
            "entradas": entradas_manuais,
            "despesas": despesas_manuais,
            "contas_recebidas": contas_recebidas,
            "contas_pagas": contas_pagas,
        }

    periodo = (data_inicio, data_fim)
    dre_periodo = _dre_periodo(dt_inicio, dt_fim)
    receita_bruta = dre_periodo["receita_bruta"]
    descontos = dre_periodo["descontos"]
    receita_liquida = dre_periodo["receita_liquida"]
    ticket_medio = dre_periodo["ticket_medio"]
    qtd_vendas = dre_periodo["qtd_vendas"]
    entradas = dre_periodo["entradas"]
    despesas = dre_periodo["despesas"]
    saldo_periodo = receita_liquida + entradas - despesas

    cursor.execute("""
        SELECT
            COUNT(*) AS qtd_cancelamentos,
            COALESCE(SUM(total), 0) AS total_cancelado
        FROM movimentos
        WHERE status='CANCELADO'
          AND substr(COALESCE(data_cancelamento, data_abertura), 1, 10) BETWEEN ? AND ?
    """, periodo)
    cancel = cursor.fetchone()
    qtd_cancelamentos = int(cancel["qtd_cancelamentos"] or 0)
    total_cancelado = float(cancel["total_cancelado"] or 0)

    cursor.execute("""
        SELECT COALESCE(forma_pagamento, 'NAO INFORMADO') AS forma,
               COALESCE(SUM(total), 0) AS total
        FROM movimentos
        WHERE status='PAGO'
          AND substr(data_abertura, 1, 10) BETWEEN ? AND ?
        GROUP BY forma_pagamento
        ORDER BY total DESC
    """, periodo)
    formas_rows = cursor.fetchall()
    formas_pagamento = [
        {"forma": f["forma"], "total": float(f["total"] or 0)}
        for f in formas_rows
    ]

    cursor.execute("""
        SELECT
            fl.id,
            fl.tipo,
            fl.categoria,
            fl.descricao,
            fl.valor,
            fl.data_lancamento,
            fl.caixa_id,
            COALESCE(f.nome, '') AS funcionario_nome
        FROM financeiro_lancamentos fl
        LEFT JOIN funcionarios f ON f.id = fl.funcionario_id
        WHERE fl.data_lancamento BETWEEN ? AND ?
        ORDER BY fl.data_lancamento DESC, fl.id DESC
        LIMIT 80
    """, periodo)
    lancamentos = cursor.fetchall()

    cursor.execute("""
        SELECT
            substr(data_abertura, 1, 10) AS dia,
            COALESCE(SUM(total), 0) AS total
        FROM movimentos
        WHERE status='PAGO'
          AND substr(data_abertura, 1, 10) BETWEEN ? AND ?
        GROUP BY substr(data_abertura, 1, 10)
        ORDER BY dia
    """, periodo)
    receita_dia_map = {r["dia"]: float(r["total"] or 0) for r in cursor.fetchall()}

    cursor.execute("""
        SELECT
            data_lancamento AS dia,
            COALESCE(SUM(CASE WHEN tipo='ENTRADA' THEN valor ELSE 0 END), 0) AS entradas,
            COALESCE(SUM(CASE WHEN tipo='DESPESA' THEN valor ELSE 0 END), 0) AS despesas
        FROM financeiro_lancamentos
        WHERE data_lancamento BETWEEN ? AND ?
        GROUP BY data_lancamento
        ORDER BY data_lancamento
    """, periodo)
    lanc_dia_rows = cursor.fetchall()
    lanc_dia_map = {
        r["dia"]: {
            "entradas": float(r["entradas"] or 0),
            "despesas": float(r["despesas"] or 0),
        }
        for r in lanc_dia_rows
    }

    fluxo_diario = []
    dias = sorted(set(list(receita_dia_map.keys()) + list(lanc_dia_map.keys())))
    saldo_acumulado = 0.0
    for dia in dias:
        receita_dia = float(receita_dia_map.get(dia, 0))
        entradas_dia = float(lanc_dia_map.get(dia, {}).get("entradas", 0))
        despesas_dia = float(lanc_dia_map.get(dia, {}).get("despesas", 0))
        saldo_dia = receita_dia + entradas_dia - despesas_dia
        saldo_acumulado += saldo_dia
        fluxo_diario.append({
            "dia": dia,
            "receita": receita_dia,
            "entradas": entradas_dia,
            "despesas": despesas_dia,
            "saldo_dia": saldo_dia,
            "saldo_acumulado": saldo_acumulado,
        })

    mes_atual_ini = hoje.replace(day=1)
    mes_atual_fim = _fim_mes(mes_atual_ini)
    mes_anterior_ref = mes_atual_ini - timedelta(days=1)
    mes_anterior_ini = mes_anterior_ref.replace(day=1)
    mes_anterior_fim = _fim_mes(mes_anterior_ini)
    dre_mes_atual = _dre_periodo(mes_atual_ini, mes_atual_fim)
    dre_mes_anterior = _dre_periodo(mes_anterior_ini, mes_anterior_fim)

    conta_status = (request.args.get("conta_status") or "TODOS").strip().upper()
    conta_tipo = (request.args.get("conta_tipo") or "TODOS").strip().upper()
    filtros = ["1=1"]
    params = []
    if conta_status in ("PENDENTE", "PAGO"):
        filtros.append("cf.status=?")
        params.append(conta_status)
    if conta_tipo in ("PAGAR", "RECEBER"):
        filtros.append("cf.tipo=?")
        params.append(conta_tipo)

    cursor.execute(f"""
        SELECT
            cf.id, cf.tipo, cf.categoria, cf.descricao, cf.valor,
            cf.data_emissao, cf.data_vencimento, cf.status, cf.data_pagamento,
            cf.forma_pagamento, cf.observacao,
            COALESCE(f.nome, '') AS funcionario_nome
        FROM contas_financeiras cf
        LEFT JOIN funcionarios f ON f.id = cf.funcionario_id
        WHERE {' AND '.join(filtros)}
        ORDER BY cf.data_vencimento ASC, cf.id DESC
        LIMIT 120
    """, params)
    contas = cursor.fetchall()

    hoje_str = hoje.strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN tipo='PAGAR' AND status='PENDENTE' THEN valor ELSE 0 END), 0) AS pagar_pendente,
            COALESCE(SUM(CASE WHEN tipo='RECEBER' AND status='PENDENTE' THEN valor ELSE 0 END), 0) AS receber_pendente,
            COALESCE(SUM(CASE WHEN tipo='PAGAR' AND status='PENDENTE' AND data_vencimento < ? THEN valor ELSE 0 END), 0) AS pagar_atrasado,
            COALESCE(SUM(CASE WHEN tipo='RECEBER' AND status='PENDENTE' AND data_vencimento < ? THEN valor ELSE 0 END), 0) AS receber_atrasado
        FROM contas_financeiras
    """, (hoje_str, hoje_str))
    resumo_contas = cursor.fetchone()

    meses = []
    ref = mes_atual_ini
    for _ in range(6):
        ini = ref.replace(day=1)
        fim = _fim_mes(ini)
        dre_m = _dre_periodo(ini, fim)
        meses.append({
            "mes": ini.strftime("%Y-%m"),
            "receita": dre_m["receita_liquida"],
            "despesas": dre_m["despesas_operacionais"] + dre_m["cmv"],
            "resultado": dre_m["resultado"],
        })
        ref = ini - timedelta(days=1)
    meses.reverse()

    mes_meta = mes_atual_ini.strftime("%Y-%m")
    cursor.execute("""
        SELECT ano_mes, COALESCE(meta_receita, 0) AS meta_receita, COALESCE(meta_resultado, 0) AS meta_resultado
        FROM financeiro_metas
        WHERE ano_mes=?
        LIMIT 1
    """, (mes_meta,))
    meta_atual = cursor.fetchone()
    if not meta_atual:
        meta_atual = {
            "ano_mes": mes_meta,
            "meta_receita": 0.0,
            "meta_resultado": 0.0,
        }

    receita_atual = float(dre_mes_atual["receita_liquida"] or 0)
    resultado_atual = float(dre_mes_atual["resultado"] or 0)
    meta_receita_val = float(meta_atual["meta_receita"] or 0)
    meta_resultado_val = float(meta_atual["meta_resultado"] or 0)

    perc_receita = (receita_atual / meta_receita_val * 100.0) if meta_receita_val > 0 else 0.0
    perc_resultado = (resultado_atual / meta_resultado_val * 100.0) if meta_resultado_val > 0 else 0.0

    def _semaforo(perc):
        if perc >= 100:
            return "verde"
        if perc >= 80:
            return "amarelo"
        return "vermelho"

    meta_status = {
        "receita": _semaforo(perc_receita) if meta_receita_val > 0 else "sem_meta",
        "resultado": _semaforo(perc_resultado) if meta_resultado_val > 0 else "sem_meta",
    }

    conn.close()

    return render_template(
        "financeiro.html",
        data_inicio=data_inicio,
        data_fim=data_fim,
        receita_bruta=receita_bruta,
        descontos=descontos,
        receita_liquida=receita_liquida,
        despesas=despesas,
        entradas=entradas,
        saldo_periodo=saldo_periodo,
        qtd_vendas=qtd_vendas,
        ticket_medio=ticket_medio,
        qtd_cancelamentos=qtd_cancelamentos,
        total_cancelado=total_cancelado,
        formas_pagamento=formas_pagamento,
        lancamentos=lancamentos,
        fluxo_diario=fluxo_diario,
        hoje=fim_padrao,
        dre_periodo=dre_periodo,
        dre_mes_atual=dre_mes_atual,
        dre_mes_anterior=dre_mes_anterior,
        contas=contas,
        conta_status=conta_status,
        conta_tipo=conta_tipo,
        resumo_contas=resumo_contas,
        historico_mensal=meses,
        meta_atual=meta_atual,
        meta_status=meta_status,
        perc_meta_receita=perc_receita,
        perc_meta_resultado=perc_resultado,
    )


@app.route("/financeiro/excluir/<int:lancamento_id>", methods=["POST"])
def excluir_lancamento_financeiro(lancamento_id):
    ensure_financeiro_table()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM financeiro_lancamentos WHERE id=?", (lancamento_id,))
    conn.commit()
    conn.close()
    return redirect("/financeiro")


@app.route("/financeiro/meta", methods=["POST"])
def salvar_meta_financeira():
    ensure_financeiro_metas_table()
    conn = get_db()
    cursor = conn.cursor()

    ano_mes = (request.form.get("ano_mes") or "").strip()
    try:
        datetime.strptime(f"{ano_mes}-01", "%Y-%m-%d")
    except ValueError:
        conn.close()
        return "Mes de referencia invalido.", 400

    meta_receita = parse_float_br(request.form.get("meta_receita"), 0)
    meta_resultado = parse_float_br(request.form.get("meta_resultado"), 0)

    meta_receita = max(0.0, meta_receita)
    meta_resultado = max(0.0, meta_resultado)

    cursor.execute("""
        INSERT INTO financeiro_metas (ano_mes, meta_receita, meta_resultado, funcionario_id)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ano_mes) DO UPDATE SET
            meta_receita=excluded.meta_receita,
            meta_resultado=excluded.meta_resultado,
            atualizado_em=CURRENT_TIMESTAMP,
            funcionario_id=excluded.funcionario_id
    """, (ano_mes, meta_receita, meta_resultado, session.get("user_id")))
    conn.commit()
    conn.close()
    return redirect("/financeiro")


@app.route("/financeiro/contas", methods=["POST"])
def criar_conta_financeira():
    ensure_contas_financeiras_table()
    conn = get_db()
    cursor = conn.cursor()

    tipo = (request.form.get("tipo") or "").strip().upper()
    categoria = (request.form.get("categoria") or "").strip()
    descricao = (request.form.get("descricao") or "").strip()
    data_emissao = (request.form.get("data_emissao") or "").strip()
    data_vencimento = (request.form.get("data_vencimento") or "").strip()
    observacao = (request.form.get("observacao") or "").strip()
    valor = parse_float_br(request.form.get("valor"), 0)

    if tipo not in ("PAGAR", "RECEBER"):
        conn.close()
        return "Tipo de conta invalido.", 400
    if valor <= 0:
        conn.close()
        return "Valor da conta deve ser maior que zero.", 400
    try:
        datetime.strptime(data_emissao, "%Y-%m-%d")
        datetime.strptime(data_vencimento, "%Y-%m-%d")
    except ValueError:
        conn.close()
        return "Datas invalidas.", 400

    cursor.execute("""
        INSERT INTO contas_financeiras
        (tipo, categoria, descricao, valor, data_emissao, data_vencimento, status, observacao, funcionario_id)
        VALUES (?, ?, ?, ?, ?, ?, 'PENDENTE', ?, ?)
    """, (
        tipo, categoria, descricao, valor, data_emissao, data_vencimento, observacao, session.get("user_id")
    ))
    conn.commit()
    conn.close()
    return redirect("/financeiro")


@app.route("/financeiro/contas/<int:conta_id>/baixar", methods=["POST"])
def baixar_conta_financeira(conta_id):
    ensure_contas_financeiras_table()
    conn = get_db()
    cursor = conn.cursor()
    data_pagamento = (request.form.get("data_pagamento") or datetime.now().strftime("%Y-%m-%d")).strip()
    forma_pagamento = (request.form.get("forma_pagamento") or "").strip().upper()
    if forma_pagamento and forma_pagamento not in ("DINHEIRO", "PIX", "CARTAO", "TRANSFERENCIA", "BOLETO", "DEBITO"):
        forma_pagamento = "OUTROS"

    try:
        datetime.strptime(data_pagamento, "%Y-%m-%d")
    except ValueError:
        conn.close()
        return "Data de pagamento invalida.", 400

    cursor.execute("SELECT id FROM contas_financeiras WHERE id=?", (conta_id,))
    if not cursor.fetchone():
        conn.close()
        return "Conta nao encontrada.", 404

    cursor.execute("""
        UPDATE contas_financeiras
        SET status='PAGO',
            data_pagamento=?,
            forma_pagamento=?,
            funcionario_id=?
        WHERE id=?
    """, (data_pagamento, forma_pagamento, session.get("user_id"), conta_id))
    conn.commit()
    conn.close()
    return redirect("/financeiro")


@app.route("/financeiro/contas/<int:conta_id>/estornar", methods=["POST"])
def estornar_conta_financeira(conta_id):
    ensure_contas_financeiras_table()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE contas_financeiras
        SET status='PENDENTE',
            data_pagamento=NULL,
            forma_pagamento=NULL
        WHERE id=?
    """, (conta_id,))
    conn.commit()
    conn.close()
    return redirect("/financeiro")


@app.route("/financeiro/contas/<int:conta_id>/excluir", methods=["POST"])
def excluir_conta_financeira(conta_id):
    ensure_contas_financeiras_table()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM contas_financeiras WHERE id=?", (conta_id,))
    conn.commit()
    conn.close()
    return redirect("/financeiro")
#=================================================
#rota para listar pedidos
#=================================================
@app.route("/caixa_pedidos")
def caixa_pedidos():
    ensure_movimentos_cancelamento_columns()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, total, data_abertura
        FROM movimentos
        WHERE status='PENDENTE'
        ORDER BY id DESC
    """)

    pedidos = cursor.fetchall()
    conn.close()
    return render_template("caixa_pedidos.html", pedidos=pedidos)
#=================================================
#rota para receber pagamento
#=================================================
@app.route("/pagar_pedido/<int:id>", methods=["GET","POST"])
def pagar_pedido(id):
    ensure_movimentos_troco_columns()
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        valor_recebido = 0.0
        if request.is_json:
            dados = request.get_json(silent=True) or {}
            forma = (dados.get("forma_pagamento") or "").strip().upper()
            bandeira = (dados.get("bandeira") or "").strip()
            try:
                valor_recebido = float(dados.get("valor_recebido") or 0)
            except (TypeError, ValueError):
                valor_recebido = 0.0
        else:
            forma = (request.form.get("forma_pagamento") or "").strip().upper()
            bandeira = request.form.get("bandeira")
            try:
                valor_recebido = float(request.form.get("valor_recebido") or 0)
            except (TypeError, ValueError):
                valor_recebido = 0.0

        if forma not in ("DINHEIRO", "PIX", "CARTAO"):
            conn.close()
            if request.is_json:
                return jsonify({"ok": False, "erro": "Forma de pagamento invalida."}), 400
            return "Forma de pagamento invalida", 400

        cursor.execute("SELECT total, status FROM movimentos WHERE id=? LIMIT 1", (id,))
        pedido = cursor.fetchone()
        if not pedido:
            conn.close()
            if request.is_json:
                return jsonify({"ok": False, "erro": "Pedido nao encontrado."}), 404
            return "Pedido nao encontrado", 404
        if pedido["status"] != "PENDENTE":
            conn.close()
            if request.is_json:
                return jsonify({"ok": False, "erro": "Pedido nao esta pendente."}), 400
            return "Pedido nao esta pendente", 400

        total_pedido = float(pedido["total"] or 0)
        troco = 0.0
        if forma == "DINHEIRO":
            if valor_recebido <= 0:
                conn.close()
                if request.is_json:
                    return jsonify({"ok": False, "erro": "Informe o valor recebido em dinheiro."}), 400
                return "Informe o valor recebido", 400
            if valor_recebido < total_pedido:
                conn.close()
                if request.is_json:
                    return jsonify({"ok": False, "erro": "Valor recebido menor que o total do pedido."}), 400
                return "Valor recebido insuficiente", 400
            troco = valor_recebido - total_pedido

        # verificar caixa aberto
        cursor.execute("SELECT id FROM caixas WHERE status='ABERTO' LIMIT 1")
        caixa = cursor.fetchone()

        if not caixa:
            conn.close()
            if request.is_json:
                return jsonify({"ok": False, "erro": "Nenhum caixa aberto."}), 400
            return "Nenhum caixa aberto!"

        caixa_id = caixa[0]

        # ATUALIZA MOVIMENTO E VINCULA AO CAIXA
        cursor.execute("""
            UPDATE movimentos
            SET status='PAGO',
                forma_pagamento=?,
                bandeira=?,
                valor_recebido=?,
                troco=?,
                caixa_id=?
            WHERE id=?
        """, (forma, bandeira, valor_recebido, troco, caixa_id, id))

        cursor.execute("""
            UPDATE ordem_servico
            SET status='Finalizado'
            WHERE movimento_id=?
        """, (id,))

        conn.commit()
        conn.close()

        if request.is_json:
            return jsonify({
                "ok": True,
                "cupom_url": f"/imprimir_cupom/{id}",
                "troco": float(troco),
                "valor_recebido": float(valor_recebido)
            })

        return redirect(f"/imprimir_cupom/{id}")

    conn.close()
    return render_template("pagamento.html", pedido_id=id)
#=================================================
#rota para cancelar pedido
#=================================================
@app.route("/cancelar_pedido/<int:id>", methods=["POST"])
def cancelar_pedido(id):
    from datetime import datetime

    ensure_movimentos_cancelamento_columns()

    motivo = (request.form.get("motivo_cancelamento") or "").strip()
    if not motivo:
        dados = request.get_json(silent=True) or {}
        motivo = (dados.get("motivo_cancelamento") or "").strip()

    if len(motivo) < 3:
        return jsonify({"ok": False, "erro": "Informe o motivo do cancelamento."}), 400

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT status FROM movimentos WHERE id=?", (id,))
    mov = cursor.fetchone()
    if not mov:
        conn.close()
        return jsonify({"ok": False, "erro": "Pedido nao encontrado."}), 404

    if mov["status"] != "PENDENTE":
        conn.close()
        return jsonify({"ok": False, "erro": "So pedidos pendentes podem ser cancelados."}), 400

    cursor.execute("""
        SELECT produto_id, quantidade
        FROM itens_movimento
        WHERE movimento_id=?
    """, (id,))
    itens = cursor.fetchall()

    for item in itens:
        cursor.execute("""
            UPDATE produtos
            SET estoque = estoque + ?
            WHERE id = ?
        """, (item["quantidade"], item["produto_id"]))

    data_cancelamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        UPDATE movimentos
        SET status='CANCELADO',
            motivo_cancelamento=?,
            data_cancelamento=?
        WHERE id=?
    """, (motivo, data_cancelamento, id))

    cursor.execute("""
        UPDATE ordem_servico
        SET status='Agendado'
        WHERE movimento_id=?
    """, (id,))

    conn.commit()
    conn.close()

    return jsonify({"ok": True})
#=================================================
#rota gerar_pedido
#=================================================
@app.route("/gerar_pedido", methods=["POST"])
def gerar_pedido():
    ensure_movimentos_desconto_columns()
    ensure_funcionarios_auth_columns()
    ensure_desconto_config_table()

    data = request.get_json(silent=True) or {}
    itens = data.get("itens") or []
    if not isinstance(itens, list) or not itens:
        return jsonify({"ok": False, "erro": "Pedido sem itens."}), 400

    funcionario_id = data.get("funcionario_id")
    funcionario_senha = data.get("funcionario_senha") or ""
    desconto_tipo = (data.get("desconto_tipo") or "valor").lower()
    desconto_input = data.get("desconto_valor") or 0
    autorizador_id = data.get("autorizador_id")
    autorizador_senha = data.get("autorizador_senha") or ""

    try:
        funcionario_id = int(funcionario_id)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "erro": "Informe o ID do funcionario."}), 400

    conn = get_db()
    cursor = conn.cursor()
    cfg = get_desconto_config(cursor)

    cursor.execute("""
        SELECT id, nome, senha_hash, status
        FROM funcionarios
        WHERE id=?
        LIMIT 1
    """, (funcionario_id,))
    funcionario = cursor.fetchone()

    if not funcionario:
        conn.close()
        return jsonify({"ok": False, "erro": "Funcionario nao encontrado."}), 404
    if (funcionario["status"] or "Ativo") != "Ativo":
        conn.close()
        return jsonify({"ok": False, "erro": "Funcionario inativo."}), 400
    if not funcionario["senha_hash"] or not check_password_hash(funcionario["senha_hash"], funcionario_senha):
        conn.close()
        return jsonify({"ok": False, "erro": "Senha do funcionario invalida."}), 401

    from datetime import datetime
    data_abertura = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    itens_normalizados = []
    total_bruto = 0.0

    for item in itens:
        if not isinstance(item, dict):
            conn.close()
            return jsonify({"ok": False, "erro": "Formato de item invalido."}), 400

        try:
            item_id = int(item.get("id"))
        except (TypeError, ValueError):
            conn.close()
            return jsonify({"ok": False, "erro": "Item com ID invalido."}), 400

        try:
            quantidade = int(item.get("quantidade"))
        except (TypeError, ValueError):
            conn.close()
            return jsonify({"ok": False, "erro": "Quantidade invalida."}), 400

        if quantidade < 1:
            conn.close()
            return jsonify({"ok": False, "erro": "Quantidade deve ser maior que zero."}), 400

        tipo_item = (item.get("tipo") or "produto").strip().lower()

        if tipo_item == "servico":
            cursor.execute("""
                SELECT id, nome, preco
                FROM servicos
                WHERE id=?
                LIMIT 1
            """, (item_id,))
            cadastro = cursor.fetchone()
            if not cadastro:
                conn.close()
                return jsonify({"ok": False, "erro": f"Servico {item_id} nao encontrado."}), 404

            preco_unitario = float(cadastro["preco"] or 0)
            subtotal = preco_unitario * quantidade
            itens_normalizados.append({
                "id": item_id,
                "tipo": "servico",
                "quantidade": quantidade,
                "preco_unitario": preco_unitario,
                "subtotal": subtotal
            })
            total_bruto += subtotal
            continue

        cursor.execute("""
            SELECT id, nome, preco_venda, estoque
            FROM produtos
            WHERE id=?
            LIMIT 1
        """, (item_id,))
        cadastro = cursor.fetchone()
        if not cadastro:
            conn.close()
            return jsonify({"ok": False, "erro": f"Produto {item_id} nao encontrado."}), 404

        estoque_atual = float(cadastro["estoque"] or 0)
        if estoque_atual < quantidade:
            conn.close()
            return jsonify({"ok": False, "erro": f"Estoque insuficiente para {cadastro['nome']}."}), 400

        preco_unitario = float(cadastro["preco_venda"] or 0)
        subtotal = preco_unitario * quantidade
        itens_normalizados.append({
            "id": item_id,
            "tipo": "produto",
            "quantidade": quantidade,
            "preco_unitario": preco_unitario,
            "subtotal": subtotal
        })
        total_bruto += subtotal

    try:
        desconto_input = float(desconto_input)
    except (TypeError, ValueError):
        desconto_input = 0.0
    if desconto_input < 0:
        desconto_input = 0.0

    desconto_valor = 0.0
    desconto_tipo_db = None
    autorizador_id_db = None

    if desconto_tipo == "percentual":
        if desconto_input > 100:
            conn.close()
            return jsonify({"ok": False, "erro": "Desconto percentual nao pode ser maior que 100."}), 400
        desconto_valor = total_bruto * (desconto_input / 100.0)
        desconto_tipo_db = "PERCENTUAL"
    else:
        desconto_valor = desconto_input
        desconto_tipo_db = "VALOR"

    if desconto_valor > total_bruto:
        desconto_valor = total_bruto

    percentual_real = (desconto_valor / total_bruto * 100.0) if total_bruto > 0 else 0.0
    limite_func_percent = float(cfg["limite_funcionario_percent"] or 0)
    limite_func_valor = float(cfg["limite_funcionario_valor"] or 0)
    exige_autorizacao = desconto_valor > 0 and (
        desconto_valor > limite_func_valor or percentual_real > limite_func_percent
    )

    if desconto_valor > 0:
        if exige_autorizacao:
            try:
                autorizador_id_db = int(autorizador_id)
            except (TypeError, ValueError):
                conn.close()
                return jsonify({"ok": False, "erro": "Desconto exige ID do gerente/proprietario."}), 400

            cursor.execute("""
                SELECT id, senha_hash, perfil, status
                FROM funcionarios
                WHERE id=?
                LIMIT 1
            """, (autorizador_id_db,))
            autorizador = cursor.fetchone()

            if not autorizador:
                conn.close()
                return jsonify({"ok": False, "erro": "Autorizador nao encontrado."}), 404
            if (autorizador["status"] or "Ativo") != "Ativo":
                conn.close()
                return jsonify({"ok": False, "erro": "Autorizador inativo."}), 400
            perfil_autorizador = (autorizador["perfil"] or "funcionario")
            if perfil_autorizador not in ("gerente", "proprietario"):
                conn.close()
                return jsonify({"ok": False, "erro": "Desconto so pode ser autorizado por gerente ou proprietario."}), 403
            if not autorizador["senha_hash"] or not check_password_hash(autorizador["senha_hash"], autorizador_senha):
                conn.close()
                return jsonify({"ok": False, "erro": "Senha do autorizador invalida."}), 401

            if perfil_autorizador == "gerente":
                if percentual_real > float(cfg["limite_gerente_percent"] or 0) or desconto_valor > float(cfg["limite_gerente_valor"] or 0):
                    conn.close()
                    return jsonify({"ok": False, "erro": "Desconto acima do limite permitido para gerente."}), 403
            elif perfil_autorizador == "proprietario":
                if percentual_real > float(cfg["limite_proprietario_percent"] or 0) or desconto_valor > float(cfg["limite_proprietario_valor"] or 0):
                    conn.close()
                    return jsonify({"ok": False, "erro": "Desconto acima do limite permitido para proprietario."}), 403

    total_final = total_bruto - desconto_valor

    # cria pedido pendente
    cursor.execute("""
        INSERT INTO movimentos (
            tipo, data_abertura, status, total, funcionario_id,
            desconto_valor, desconto_tipo, autorizador_id
        )
        VALUES ('VENDA', ?, 'PENDENTE', ?, ?, ?, ?, ?)
    """, (data_abertura, total_final, funcionario_id, desconto_valor, desconto_tipo_db, autorizador_id_db))

    movimento_id = cursor.lastrowid

    for item in itens_normalizados:
        cursor.execute("""
            INSERT INTO itens_movimento
            (movimento_id, produto_id, quantidade, preco_unitario, subtotal)
            VALUES (?, ?, ?, ?, ?)
        """, (
            movimento_id,
            item["id"],
            item["quantidade"],
            item["preco_unitario"],
            item["subtotal"]
        ))

        if item["tipo"] == "produto":
            cursor.execute("""
                UPDATE produtos
                SET estoque = estoque - ?
                WHERE id = ?
                  AND estoque >= ?
            """, (item["quantidade"], item["id"], item["quantidade"]))

            if cursor.rowcount != 1:
                conn.rollback()
                conn.close()
                return jsonify({"ok": False, "erro": "Estoque alterado durante a venda. Revise o carrinho e tente novamente."}), 409

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "pedido_id": movimento_id,
        "total_bruto": float(total_bruto),
        "desconto_valor": float(desconto_valor),
        "total_final": float(total_final)
    })
#=================================================
#rota que envia dados atualizados
#===============================================
@app.route("/caixa_total")
def caixa_total():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM caixas WHERE status='ABERTO' LIMIT 1")
    caixa = cursor.fetchone()

    if not caixa:
        conn.close()
        return {"total": 0, "vendas": 0}

    caixa_id = caixa["id"]

    cursor.execute("""
        SELECT SUM(total), COUNT(*)
        FROM movimentos
        WHERE caixa_id=? AND status='PAGO'
    """, (caixa_id,))

    resultado = cursor.fetchone()

    total = resultado[0] or 0
    vendas = resultado[1] or 0
    ticket_medio = (total / vendas) if vendas else 0

    cursor.execute("""
        SELECT COALESCE(forma_pagamento, 'NAO INFORMADO') AS forma,
               COALESCE(SUM(total), 0) AS total
        FROM movimentos
        WHERE caixa_id=? AND status='PAGO'
        GROUP BY forma_pagamento
    """, (caixa_id,))
    formas = cursor.fetchall()
    formas_pagamento = {f["forma"]: float(f["total"] or 0) for f in formas}

    cursor.execute("""
        SELECT substr(data_abertura, 12, 2) AS hora,
               COALESCE(SUM(total), 0) AS total
        FROM movimentos
        WHERE caixa_id=? AND status='PAGO'
        GROUP BY substr(data_abertura, 12, 2)
        ORDER BY hora
    """, (caixa_id,))
    horas = cursor.fetchall()
    vendas_por_hora = [{"hora": h["hora"], "total": float(h["total"] or 0)} for h in horas]

    payload = {
        "total": float(total),
        "vendas": int(vendas),
        "ticket_medio": float(ticket_medio),
        "formas_pagamento": formas_pagamento,
        "vendas_por_hora": vendas_por_hora
    }
    conn.close()
    return payload
# ================================================
#rota que retorna pedidos em JSON
#=================================================
@app.route("/pedidos_pendentes")
def pedidos_pendentes():
    ensure_movimentos_cancelamento_columns()
    ensure_movimentos_desconto_columns()
    ensure_ordem_servico_mobile_columns()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT m.id, m.tipo, m.total, m.data_abertura, m.funcionario_id,
               COALESCE(f.nome, '') AS funcionario_nome,
               os.id AS os_id,
               COALESCE(m.desconto_valor, 0) AS desconto_valor,
               COALESCE(m.desconto_tipo, '') AS desconto_tipo
        FROM movimentos m
        LEFT JOIN funcionarios f ON m.funcionario_id = f.id
        LEFT JOIN ordem_servico os ON os.movimento_id = m.id
        WHERE m.status='PENDENTE'
        ORDER BY m.id DESC
    """)

    pedidos = cursor.fetchall()

    lista = [
        {
            "id": p["id"],
            "origem": p["tipo"] or "VENDA",
            "os_id": p["os_id"],
            "total": p["total"],
            "data": p["data_abertura"],
            "funcionario_id": p["funcionario_id"],
            "funcionario_nome": p["funcionario_nome"],
            "desconto_valor": p["desconto_valor"],
            "desconto_tipo": p["desconto_tipo"]
        }
        for p in pedidos
    ]

    cursor.execute("""
        SELECT m.id, m.tipo, m.total, m.status, m.forma_pagamento, m.data_abertura,
               m.funcionario_id, COALESCE(f.nome, '') AS funcionario_nome,
               os.id AS os_id,
               COALESCE(m.data_cancelamento, '') AS data_cancelamento,
               COALESCE(m.motivo_cancelamento, '') AS motivo_cancelamento,
               COALESCE(m.desconto_valor, 0) AS desconto_valor,
               COALESCE(m.desconto_tipo, '') AS desconto_tipo
        FROM movimentos m
        LEFT JOIN funcionarios f ON m.funcionario_id = f.id
        LEFT JOIN ordem_servico os ON os.movimento_id = m.id
        WHERE m.status IN ('PAGO', 'CANCELADO')
        ORDER BY m.id DESC
        LIMIT 25
    """)
    fechados = cursor.fetchall()

    lista_fechados = [
        {
            "id": f["id"],
            "origem": f["tipo"] or "VENDA",
            "os_id": f["os_id"],
            "total": f["total"],
            "status": f["status"],
            "forma_pagamento": f["forma_pagamento"],
            "data": f["data_abertura"],
            "funcionario_id": f["funcionario_id"],
            "funcionario_nome": f["funcionario_nome"],
            "data_cancelamento": f["data_cancelamento"],
            "motivo_cancelamento": f["motivo_cancelamento"],
            "desconto_valor": f["desconto_valor"],
            "desconto_tipo": f["desconto_tipo"]
        }
        for f in fechados
    ]

    conn.close()
    return {"pedidos": lista, "fechados": lista_fechados}
# ================================================
#rota para ver o pedido
#=================================================
@app.route("/ver_pedido/<int:id>")
def ver_pedido(id):
    conn = get_db()
    cursor = conn.cursor()

    # dados do pedido
    cursor.execute("""
        SELECT id, total
        FROM movimentos
        WHERE id=?
    """, (id,))
    pedido = cursor.fetchone()

    # itens do pedido
    cursor.execute("""
        SELECT p.nome, im.quantidade, im.preco_unitario, im.subtotal
        FROM itens_movimento im
        JOIN produtos p ON im.produto_id = p.id
        WHERE im.movimento_id=?
    """, (id,))
    itens = cursor.fetchall()

    conn.close()
    return render_template("ver_pedido.html", pedido=pedido, itens=itens)

# ================================================
#rota detalhes pedido em JSON
#=================================================
@app.route("/pedido_detalhes/<int:id>")
def pedido_detalhes(id):
    ensure_movimentos_desconto_columns()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT m.id, m.total, m.status, m.data_abertura, m.funcionario_id,
               COALESCE(f.nome, '') AS funcionario_nome,
               COALESCE(m.desconto_valor, 0) AS desconto_valor,
               COALESCE(m.desconto_tipo, '') AS desconto_tipo
        FROM movimentos m
        LEFT JOIN funcionarios f ON m.funcionario_id = f.id
        WHERE m.id=?
    """, (id,))
    pedido = cursor.fetchone()

    if not pedido:
        conn.close()
        return jsonify({"ok": False, "erro": "Pedido nao encontrado."}), 404

    cursor.execute("""
        SELECT p.nome, im.quantidade, im.preco_unitario, im.subtotal
        FROM itens_movimento im
        JOIN produtos p ON im.produto_id = p.id
        WHERE im.movimento_id=?
    """, (id,))
    itens = cursor.fetchall()
    conn.close()

    return jsonify({
        "ok": True,
        "pedido": {
            "id": pedido["id"],
            "total": float(pedido["total"] or 0),
            "status": pedido["status"],
            "data": pedido["data_abertura"],
            "funcionario_id": pedido["funcionario_id"],
            "funcionario_nome": pedido["funcionario_nome"],
            "desconto_valor": float(pedido["desconto_valor"] or 0),
            "desconto_tipo": pedido["desconto_tipo"]
        },
        "itens": [
            {
                "nome": i["nome"],
                "quantidade": i["quantidade"],
                "preco_unitario": float(i["preco_unitario"] or 0),
                "subtotal": float(i["subtotal"] or 0)
            }
            for i in itens
        ]
    })
# ================================================
#rota gerar_pedido
#=================================================
@app.route("/imprimir_cupom/<int:id>")
def imprimir_cupom(id):
    from datetime import datetime

    ensure_loja_config_table()
    ensure_movimentos_desconto_columns()
    ensure_movimentos_troco_columns()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT m.*, COALESCE(f.nome, '') AS funcionario_nome
        FROM movimentos m
        LEFT JOIN funcionarios f ON m.funcionario_id = f.id
        WHERE m.id=?
    """, (id,))
    pedido = cursor.fetchone()

    cursor.execute("""
        SELECT p.nome, im.quantidade, im.preco_unitario, im.subtotal
        FROM itens_movimento im
        JOIN produtos p ON im.produto_id = p.id
        WHERE im.movimento_id=?
    """, (id,))
    itens = cursor.fetchall()

    cursor.execute("SELECT * FROM loja_config WHERE id=1")
    loja = cursor.fetchone()

    data = datetime.now().strftime("%d/%m/%Y %H:%M")
    conn.close()

    return render_template("cupom.html",
                           pedido=pedido,
                           itens=itens,
                           data=data,
                           loja=loja)
# ================================================
#rota cadastro funcionarios
#=================================================
@app.route("/funcionarios", methods=["GET","POST"])
def funcionarios():
    ensure_funcionarios_auth_columns()

    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":

        nome = request.form.get("nome", "").strip()
        cpf = "".join(ch for ch in (request.form.get("cpf") or "") if ch.isdigit())
        telefone = request.form.get("telefone")
        whatsapp = request.form.get("whatsapp")
        email = request.form.get("email")
        cargo = request.form.get("cargo")
        funcao = request.form.get("funcao")
        comissao = request.form.get("comissao")
        endereco = request.form.get("endereco")
        perfil = (request.form.get("perfil") or "funcionario").lower()
        senha = request.form.get("senha") or ""

        if perfil not in ROLE_LEVEL:
            perfil = "funcionario"

        if len(cpf) < 11:
            conn.close()
            return "CPF invalido.", 400
        if len(senha) < 4:
            conn.close()
            return "Senha deve ter ao menos 4 caracteres.", 400

        senha_hash = generate_password_hash(senha)

        cursor.execute("""
        INSERT INTO funcionarios
        (nome,cpf,telefone,whatsapp,email,cargo,funcao,comissao,endereco,status,senha_hash,perfil)
        VALUES (?,?,?,?,?,?,?,?,?,'Ativo',?,?)
        """,(nome,cpf,telefone,whatsapp,email,cargo,funcao,comissao,endereco,senha_hash,perfil))

        conn.commit()

    cursor.execute("SELECT * FROM funcionarios")
    lista = cursor.fetchall()

    conn.close()

    return render_template("funcionarios.html", funcionarios=lista)


@app.route("/editar_funcionario/<int:id>", methods=["GET", "POST"])
def editar_funcionario(id):
    ensure_funcionarios_auth_columns()
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        cpf = "".join(ch for ch in (request.form.get("cpf") or "") if ch.isdigit())
        telefone = request.form.get("telefone")
        whatsapp = request.form.get("whatsapp")
        email = request.form.get("email")
        cargo = request.form.get("cargo")
        funcao = request.form.get("funcao")
        comissao = request.form.get("comissao")
        endereco = request.form.get("endereco")
        perfil = (request.form.get("perfil") or "funcionario").lower()
        senha = request.form.get("senha") or ""

        if perfil not in ROLE_LEVEL:
            perfil = "funcionario"
        if len(cpf) < 11:
            conn.close()
            return "CPF invalido.", 400

        if senha:
            senha_hash = generate_password_hash(senha)
            cursor.execute("""
                UPDATE funcionarios
                SET nome=?, cpf=?, telefone=?, whatsapp=?, email=?,
                    cargo=?, funcao=?, comissao=?, endereco=?, perfil=?,
                    senha_hash=?
                WHERE id=?
            """, (nome, cpf, telefone, whatsapp, email, cargo, funcao, comissao, endereco, perfil, senha_hash, id))
        else:
            cursor.execute("""
                UPDATE funcionarios
                SET nome=?, cpf=?, telefone=?, whatsapp=?, email=?,
                    cargo=?, funcao=?, comissao=?, endereco=?, perfil=?
                WHERE id=?
            """, (nome, cpf, telefone, whatsapp, email, cargo, funcao, comissao, endereco, perfil, id))

        conn.commit()
        conn.close()
        return redirect("/funcionarios")

    cursor.execute("SELECT * FROM funcionarios WHERE id=?", (id,))
    funcionario = cursor.fetchone()
    conn.close()

    if not funcionario:
        return "Funcionario nao encontrado.", 404

    return render_template("editar_funcionario.html", funcionario=funcionario)


@app.route("/excluir_funcionario/<int:id>", methods=["POST"])
def excluir_funcionario(id):
    ensure_funcionarios_auth_columns()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT id, perfil FROM funcionarios WHERE id=?", (id,))
    funcionario = cursor.fetchone()
    if not funcionario:
        conn.close()
        return "Funcionario nao encontrado.", 404

    usuario_logado_id = session.get("user_id")
    if usuario_logado_id == id:
        conn.close()
        return "Nao e permitido excluir o proprio usuario logado.", 400

    if (funcionario["perfil"] or "").lower() == "proprietario":
        cursor.execute("SELECT COUNT(*) FROM funcionarios WHERE perfil='proprietario'")
        total_prop = cursor.fetchone()[0]
        if total_prop <= 1:
            conn.close()
            return "Nao e permitido excluir o ultimo proprietario.", 400

    try:
        cursor.execute("DELETE FROM funcionarios WHERE id=?", (id,))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return "Nao foi possivel excluir: funcionario vinculado a outros registros.", 400

    conn.close()
    return redirect("/funcionarios")
# ================================================
#rota cadastro cliente
#=================================================
@app.route("/clientes", methods=["GET", "POST"])
def clientes():
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        from datetime import datetime

        dados = (
            request.form.get("tipo"),
            request.form.get("nome"),
            request.form.get("razao_social"),
            request.form.get("cpf"),
            request.form.get("cnpj"),
            request.form.get("telefone"),
            request.form.get("whatsapp"),
            request.form.get("email"),
            request.form.get("cep"),
            request.form.get("endereco"),
            request.form.get("numero"),
            request.form.get("bairro"),
            request.form.get("cidade"),
            request.form.get("cep_entrega"),
            request.form.get("endereco_entrega"),
            request.form.get("numero_entrega"),
            request.form.get("bairro_entrega"),
            request.form.get("cidade_entrega"),
            request.form.get("observacoes"),
            datetime.now().strftime("%Y-%m-%d")
        )

        cursor.execute("""
            INSERT INTO clientes (
                tipo, nome, razao_social, cpf, cnpj,
                telefone, whatsapp, email,
                cep, endereco, numero, bairro, cidade,
                cep_entrega, endereco_entrega, numero_entrega,
                bairro_entrega, cidade_entrega,
                observacoes, data_cadastro
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, dados)

        conn.commit()

    busca = request.args.get("busca", "").strip()

    if busca:
        cursor.execute("""
            SELECT * FROM clientes
            WHERE nome LIKE ?
            OR razao_social LIKE ?
            OR telefone LIKE ?
            OR cpf LIKE ?
            OR cnpj LIKE ?
            ORDER BY id DESC
        """, (f"%{busca}%", f"%{busca}%", f"%{busca}%", f"%{busca}%", f"%{busca}%"))
    else:
        cursor.execute("SELECT * FROM clientes ORDER BY id DESC")

    clientes = cursor.fetchall()
    conn.close()

    return render_template("clientes.html", clientes=clientes)

    if busca:
        cursor.execute("""
            SELECT * FROM clientes
            WHERE nome LIKE ?
            OR telefone LIKE ?
            OR cpf_cnpj LIKE ?
        """, (f"%{busca}%", f"%{busca}%", f"%{busca}%"))
    else:
        cursor.execute("SELECT * FROM clientes ORDER BY id DESC")

    clientes = cursor.fetchall()
    return render_template("clientes.html", clientes=clientes)
# ================================================
#rota buscar_cliente
#=================================================
@app.route("/buscar_cliente")
def buscar_cliente():
    termo = request.args.get("q", "").strip()

    conn = get_db()
    cursor = conn.cursor()

    param = f"%{termo}%"

    cursor.execute("""
        SELECT id, nome, razao_social, telefone, cidade, tipo
        FROM clientes
        WHERE
            nome LIKE ?
            OR razao_social LIKE ?
            OR telefone LIKE ?
            OR cpf LIKE ?
            OR cnpj LIKE ?
        LIMIT 10
    """, (param, param, param, param, param))

    clientes = cursor.fetchall()
    conn.close()

    resultado = []
    for c in clientes:
        resultado.append({
            "id": c["id"],
            "nome": c["razao_social"] if c["tipo"] == "PJ" else c["nome"],
            "telefone": c["telefone"],
            "cidade": c["cidade"]
        })

    return jsonify(resultado)
# ================================================
#rota cadastro rapido cliente
#=================================================
@app.route("/cadastrar_cliente_rapido", methods=["POST"])
def cadastrar_cliente_rapido():
    dados = request.get_json(silent=True) or {}

    nome = (dados.get("nome") or "").strip()
    telefone = (dados.get("telefone") or "").strip()
    whatsapp = (dados.get("whatsapp") or "").strip()
    cpf = (dados.get("cpf") or "").strip()
    endereco = (dados.get("endereco") or "").strip()
    numero = (dados.get("numero") or "").strip()

    if len(nome) < 3:
        return jsonify({"erro": "Informe um nome valido com pelo menos 3 caracteres."}), 400
    if not cpf:
        return jsonify({"erro": "CPF e obrigatorio."}), 400
    if len("".join(ch for ch in cpf if ch.isdigit())) < 11:
        return jsonify({"erro": "Informe um CPF valido."}), 400
    if not endereco:
        return jsonify({"erro": "Endereco e obrigatorio."}), 400
    if not numero:
        return jsonify({"erro": "Numero e obrigatorio."}), 400

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO clientes (
            tipo, nome, razao_social, cpf, cnpj,
            telefone, whatsapp, email,
            cep, endereco, numero, bairro, cidade,
            cep_entrega, endereco_entrega, numero_entrega,
            bairro_entrega, cidade_entrega,
            observacoes, data_cadastro
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        "PF",
        nome,
        "",
        cpf,
        "",
        telefone,
        whatsapp,
        "",
        "",
        endereco,
        numero,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "Cadastro rapido pelo movimento",
        datetime.now().strftime("%Y-%m-%d")
    ))

    conn.commit()
    cliente_id = cursor.lastrowid
    conn.close()

    return jsonify({
        "id": cliente_id,
        "nome": nome,
        "telefone": telefone,
        "whatsapp": whatsapp,
        "cpf": cpf
    })
# ================================================
#rota para abrir cliente
#=================================================
@app.route("/cliente/<int:id>")
def ver_cliente(id):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM clientes WHERE id=?", (id,))
    cliente = cursor.fetchone()

    conn.close()

    return render_template("cliente_detalhe.html", cliente=cliente)

# ================================================
#rota para excluir cliente
#=================================================
@app.route("/excluir_cliente/<int:id>", methods=["POST"])
def excluir_cliente(id):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM clientes WHERE id=?", (id,))
    conn.commit()
    conn.close()

    return redirect("/clientes")
# ================================================
#rota editar cliente
#=================================================
@app.route("/editar_cliente/<int:id>", methods=["GET","POST"])
def editar_cliente(id):
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":

        cursor.execute("""
            UPDATE clientes SET
                nome=?,
                razao_social=?,
                telefone=?,
                whatsapp=?,
                email=?,
                cpf=?,
                cnpj=?,

                cep=?,
                endereco=?,
                numero=?,
                bairro=?,
                cidade=?,

                cep_entrega=?,
                endereco_entrega=?,
                numero_entrega=?,
                bairro_entrega=?,
                cidade_entrega=?,

                observacoes=?
            WHERE id=?
        """, (
            request.form.get("nome"),
            request.form.get("razao_social"),
            request.form.get("telefone"),
            request.form.get("whatsapp"),
            request.form.get("email"),
            request.form.get("cpf"),
            request.form.get("cnpj"),

            request.form.get("cep"),
            request.form.get("endereco"),
            request.form.get("numero"),
            request.form.get("bairro"),
            request.form.get("cidade"),

            request.form.get("cep_entrega"),
            request.form.get("endereco_entrega"),
            request.form.get("numero_entrega"),
            request.form.get("bairro_entrega"),
            request.form.get("cidade_entrega"),

            request.form.get("observacoes"),
            id
        ))

        conn.commit()
        conn.close()

        return redirect(f"/cliente/{id}")

    cursor.execute("SELECT * FROM clientes WHERE id=?", (id,))
    cliente = cursor.fetchone()
    conn.close()

    return render_template("editar_cliente.html", cliente=cliente)

# ================================================
#rota criar os
#=================================================
from urllib.parse import quote
from flask import request, jsonify

@app.route("/criar_os", methods=["POST"])
def criar_os():

    dados = request.get_json()

    # dados enviados pelo javascript
    servico = dados.get("servico")
    data = dados.get("data")
    hora = dados.get("hora")
    cliente_id = dados.get("cliente_id")
    tecnico_id = dados.get("tecnico_id")

    defeito_reclamado = dados.get("defeito_reclamado")
    produto_defeito = dados.get("produto_defeito")
    observacao = dados.get("observacao")
    try:
        valor_mao_obra = float(dados.get("valor_mao_obra") or 0)
    except (TypeError, ValueError):
        valor_mao_obra = 0.0
    try:
        quantidade_mao_obra = int(dados.get("quantidade_mao_obra") or 1)
    except (TypeError, ValueError):
        quantidade_mao_obra = 1
    if quantidade_mao_obra < 1:
        quantidade_mao_obra = 1

    conn = get_db()
    cursor = conn.cursor()

    # salvar ordem de serviço
    cursor.execute("""
        INSERT INTO ordem_servico
        (
            servico_nome,
            data_agendamento,
            hora,
            status,
            cliente_id,
            tecnico_id,
            defeito_reclamado,
            produto_defeito,
            observacao,
            data_criacao
        )
        VALUES (?, ?, ?, 'Agendado', ?, ?, ?, ?, ?, datetime('now'))
    """,(
        servico,
        data,
        hora,
        cliente_id,
        tecnico_id,
        defeito_reclamado,
        produto_defeito,
        observacao
    ))

    # pegar ID da OS criada
    os_id = cursor.lastrowid

    # se veio serviço do PDV, já lança a mão de obra na OS
    if valor_mao_obra > 0:
        subtotal_mao_obra = float(valor_mao_obra) * int(quantidade_mao_obra)
        cursor.execute("""
            INSERT INTO itens_ordem_servico
            (os_id, descricao, quantidade, valor, subtotal)
            VALUES (?, ?, ?, ?, ?)
        """, (
            os_id,
            f"Mao de obra - {servico}",
            quantidade_mao_obra,
            float(valor_mao_obra),
            subtotal_mao_obra
        ))

    conn.commit()

    # buscar cliente
    cursor.execute("""
        SELECT nome, whatsapp
        FROM clientes
        WHERE id = ?
    """,(cliente_id,))

    cliente = cursor.fetchone()

    nome_cliente = cliente["nome"]
    telefone = cliente["whatsapp"]

    # mensagem para whatsapp
    mensagem = f"""
Olá {nome_cliente}!

Sua Ordem de Serviço foi agendada.

OS Nº {os_id}
Serviço: {servico}
Data: {data}
Hora: {hora}

CentralMarket Assistência Técnica
"""

    mensagem_url = quote(mensagem)

    link_whatsapp = f"https://wa.me/55{telefone}?text={mensagem_url}"

    return jsonify({
        "status": "ok",
        "os_id": os_id,
        "whatsapp": link_whatsapp
    })
# ================================================
#rota ordens serviço
#=================================================
@app.route("/ordens_servico")
def ordens_servico():

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT 
        os.id,
        os.servico_nome,
        os.data_agendamento,
        os.status,
        c.nome,
        c.telefone,
        c.endereco,
        c.numero,
        c.bairro,
        c.cidade
    FROM ordem_servico os
    LEFT JOIN clientes c ON os.cliente_id = c.id
    ORDER BY os.data_agendamento
    """)

    ordens = cursor.fetchall()

    conn.close()

    return render_template("ordens_servico.html", ordens=ordens)
# ================================================
#rota agenda do serviço hoje
#=================================================
@app.route("/agenda")
def agenda():

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT id, nome FROM funcionarios")
    tecnicos = cursor.fetchall()

    conn.close()

    return render_template("agenda_calendario.html", tecnicos=tecnicos)
# ================================================
#rota para atualizar status do serviços
#=================================================
@app.route("/atualizar_status/<int:id>/<status>")
def atualizar_status(id, status):

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE ordem_servico
        SET status = ?
        WHERE id = ?
    """, (status, id))

    conn.commit()
    conn.close()

    return redirect("/ordens_servico")
# ================================================
#rota agenda_semana
#=================================================
@app.route("/agenda_semana")
def agenda_semana():

    conn = get_db()
    cursor = conn.cursor()
    tecnico = (request.args.get("tecnico") or "").strip()

    query = """
        SELECT
            os.id,
            os.servico_nome,
            os.data_agendamento,
            os.hora,
            os.status,
            c.nome AS cliente,
            f.nome AS tecnico
        FROM ordem_servico os
        LEFT JOIN clientes c ON os.cliente_id = c.id
        LEFT JOIN funcionarios f ON os.tecnico_id = f.id
    """
    params = []
    if tecnico:
        query += " WHERE COALESCE(f.nome, '') = ?"
        params.append(tecnico)
    query += " ORDER BY os.data_agendamento, os.hora"

    cursor.execute(query, params)
    agenda = cursor.fetchall()

    cursor.execute("""
        SELECT nome
        FROM funcionarios
        WHERE status='Ativo'
        ORDER BY nome
    """)
    tecnicos = cursor.fetchall()

    conn.close()

    dias = {}

    for os in agenda:
        data = os["data_agendamento"]

        if data not in dias:
            dias[data] = []

        dias[data].append(os)

    return render_template(
        "agenda_semana.html",
        dias=dias,
        tecnicos=tecnicos,
        tecnico_atual=tecnico
    )
# ================================================
#rota agenda_dia
#=================================================
from datetime import datetime

@app.route("/agenda_dia")
def agenda_dia():

    hoje = datetime.now().strftime("%Y-%m-%d")

    tecnico = request.args.get("tecnico")

    conn = get_db()
    cursor = conn.cursor()

    query = """
    SELECT
        os.id,
        os.servico_nome,
        os.hora,
        os.status,
        c.nome AS cliente,
        c.telefone,
        c.endereco,
        c.numero,
        c.bairro,
        c.cidade,
        f.nome AS tecnico
    FROM ordem_servico os
    LEFT JOIN clientes c ON os.cliente_id = c.id
    LEFT JOIN funcionarios f ON os.tecnico_id = f.id
    WHERE os.data_agendamento = ?
    """

    params = [hoje]

    if tecnico:
        query += " AND f.nome = ?"
        params.append(tecnico)

    query += " ORDER BY f.nome, os.hora"

    cursor.execute(query, params)

    agenda = cursor.fetchall()

    cursor.execute("SELECT nome FROM funcionarios")
    tecnicos = cursor.fetchall()

    conn.close()

    return render_template(
        "agenda_dia.html",
        agenda=agenda,
        hoje=hoje,
        tecnicos=tecnicos
    )
# ================================================
#rota para mudar status
#=================================================
@app.route("/mudar_status/<int:id>/<status>")
def mudar_status(id, status):

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE ordem_servico
        SET status = ?
        WHERE id = ?
    """, (status, id))

    conn.commit()
    conn.close()
    tecnico = (request.args.get("tecnico") or "").strip()
    if tecnico:
        return redirect(url_for("agenda_dia", tecnico=tecnico))
    return redirect("/agenda_dia")
# ================================================
#rota para listar técnicos
#=================================================
@app.route("/listar_tecnicos")
def listar_tecnicos():

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT id,nome FROM funcionarios")

    rows = cursor.fetchall()

    conn.close()

    tecnicos = []

    for r in rows:
        tecnicos.append({
            "id": r["id"],
            "nome": r["nome"]
        })

    return jsonify(tecnicos)
# ================================================
#rota para listar cliente
#=================================================
@app.route("/listar_clientes")
def listar_clientes():

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT id,nome FROM clientes")

    rows = cursor.fetchall()

    conn.close()

    clientes = []

    for r in rows:
        clientes.append({
            "id": r["id"],
            "nome": r["nome"]
        })

    return jsonify(clientes)
# ================================================
#rota gerar_pedido
#=================================================
# 🔹 Envia eventos para o calendário
@app.route("/eventos_agenda")
def eventos_agenda():

    tecnico = request.args.get("tecnico")

    conn = get_db()
    cursor = conn.cursor()

    query = """
    SELECT
        os.id,
        os.servico_nome,
        os.data_agendamento,
        os.hora,
        os.status,
        c.nome AS cliente,
        f.nome AS tecnico
    FROM ordem_servico os
    LEFT JOIN clientes c ON os.cliente_id = c.id
    LEFT JOIN funcionarios f ON os.tecnico_id = f.id
    """

    params = []

    if tecnico:
        query += " WHERE os.tecnico_id = ?"
        params.append(tecnico)

    cursor.execute(query, params)

    dados = cursor.fetchall()

    eventos = []

    for os in dados:

        data = os["data_agendamento"]

        if os["hora"]:
            data = f"{data}T{os['hora']}"

        eventos.append({
            "id": os["id"],
            "title": os["servico_nome"],
            "start": data,
            "status": os["status"] or "Agendado",
            "cliente": os["cliente"],
            "tecnico": os["tecnico"]
        })

    conn.close()

    return jsonify(eventos)


# 🔹 Atualizar quando arrastar evento
@app.route("/mover_evento", methods=["POST"])
def mover_evento():

    dados = request.get_json()

    id = dados["id"]
    data = dados["data"]

    partes = data.split("T")

    nova_data = partes[0]
    nova_hora = None

    if len(partes) > 1:
        nova_hora = partes[1][:5]

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE ordem_servico
        SET data_agendamento = ?, hora = ?
        WHERE id = ?
    """, (nova_data, nova_hora, id))

    conn.commit()
    conn.close()

    return jsonify({"ok": True})
# ================================================
#rota gerar_pedido
#=================================================
@app.route("/salvar_diagnostico/<int:id>", methods=["POST"])
def salvar_diagnostico(id):

    diagnostico = request.form.get("diagnostico")
    solucao = request.form.get("solucao")
    pecas = request.form.get("pecas_trocadas")

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE ordem_servico
    SET diagnostico=?,
        solucao=?,
        pecas_trocadas=?
    WHERE id=?
    """,(diagnostico, solucao, pecas, id))

    conn.commit()
    conn.close()

    return redirect("/ordens_servico")
# ================================================
#rota ordem
#=================================================
@app.route("/ordem/<int:id>")
def ver_ordem(id):

    conn = get_db()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # buscar ordem de serviço + cliente
    cursor.execute("""
    SELECT os.*, 
           c.nome,
           c.telefone,
           c.endereco,
           c.numero,
           c.bairro,
           c.cidade
    FROM ordem_servico os
    LEFT JOIN clientes c ON os.cliente_id = c.id
    WHERE os.id = ?
    """, (id,))

    ordem = cursor.fetchone()

    # buscar itens da ordem
    cursor.execute("""
    SELECT *
    FROM itens_ordem_servico
    WHERE os_id = ?
    """, (id,))

    itens = cursor.fetchall()

    conn.close()

    return render_template(
        "ver_ordem.html",
        ordem=ordem,
        itens=itens
    )
# ================================================
#rota editar_os
#=================================================
@app.route("/editar_os/<int:id>")
def editar_os(id):

    conn = get_db()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM ordem_servico WHERE id=?", (id,))
    ordem = cursor.fetchone()

    conn.close()

    return render_template("editar_os.html", ordem=ordem)

# ================================================
#rota para salvar os
#=================================================
@app.route("/salvar_os/<int:id>", methods=["POST"])
def salvar_os(id):

    diagnostico = request.form.get("diagnostico")
    solucao = request.form.get("solucao")
    pecas = request.form.get("pecas_trocadas")

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE ordem_servico
    SET diagnostico=?, solucao=?, pecas_trocadas=?
    WHERE id=?
    """, (diagnostico, solucao, pecas, id))

    conn.commit()
    conn.close()

    return redirect(f"/ordem/{id}")
# ================================================
#rota para adicionar produto na os
#=================================================
@app.route("/adicionar_item_os/<int:os_id>", methods=["POST"])
def adicionar_item_os(os_id):

    descricao = request.form.get("descricao")
    quantidade = int(request.form.get("quantidade"))
    valor = float(request.form.get("valor"))

    subtotal = quantidade * valor

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO itens_ordem_servico
    (os_id, descricao, quantidade, valor, subtotal)
    VALUES (?, ?, ?, ?, ?)
    """,(os_id, descricao, quantidade, valor, subtotal))

    conn.commit()
    conn.close()

    return redirect(f"/ordem/{os_id}")
# ================================================
#rota gerar_pedido
#=================================================
if __name__ == "__main__":
    criar_tabelas()
    host = os.environ.get("FLASK_HOST", "0.0.0.0")
    port = int(os.environ.get("FLASK_PORT", "5000"))
    debug = (os.environ.get("FLASK_DEBUG", "1").strip().lower() in ("1", "true", "yes", "on"))

    cert_file = (os.environ.get("SSL_CERT_FILE") or "").strip()
    key_file = (os.environ.get("SSL_KEY_FILE") or "").strip()
    use_adhoc_ssl = (os.environ.get("SSL_ADHOC", "0").strip().lower() in ("1", "true", "yes", "on"))

    ssl_context = None
    if cert_file or key_file:
        if not cert_file or not key_file:
            raise RuntimeError("Para HTTPS, defina SSL_CERT_FILE e SSL_KEY_FILE juntos.")
        if not os.path.exists(cert_file):
            raise RuntimeError(f"Certificado SSL nao encontrado: {cert_file}")
        if not os.path.exists(key_file):
            raise RuntimeError(f"Chave SSL nao encontrada: {key_file}")
        ssl_context = (cert_file, key_file)
    elif use_adhoc_ssl:
        ssl_context = "adhoc"

    app.run(host=host, port=port, debug=debug, ssl_context=ssl_context)
