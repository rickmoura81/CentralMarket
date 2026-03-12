import os
import json
import re
import secrets
import time
import textwrap
import unicodedata
import sqlite3

from datetime import datetime, timedelta
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlparse

from flask import Flask, session
from werkzeug.security import generate_password_hash, check_password_hash


app = Flask(__name__)

app.secret_key = os.environ.get("SECRET_KEY") or secrets.token_hex(32)

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=os.environ.get("SESSION_COOKIE_SAMESITE", "Lax"),
    SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "0").strip().lower() in ("1", "true", "yes", "on"),
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    MAX_CONTENT_LENGTH=8 * 1024 * 1024,
)

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
    "atualizar_status",
    "mudar_status",
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
    "assistencia_tecnica",
    "assistencia_tecnica_chat",
    "assistencia_tecnica_reset",
    "assistencia_cliente_login",
    "assistencia_cliente_cadastro",
    "assistencia_cliente_logout",
    "assistencia_cliente_recuperar_senha",
    "assistencia_cliente_chamado",
}

AUTH_SCHEMA_READY = False
OS_MOBILE_SCHEMA_READY = False

UPLOAD_OS_DIR = os.path.join("static", "uploads_os")

ASSISTENCIA_SESSION_KEY = "assistencia_tecnica_chat"
CLIENTE_PORTAL_SESSION_KEY = "assistencia_cliente_portal_id"
ASSISTENCIA_SLOTS = ["09:00", "14:00", "16:00"]
ASSISTENCIA_CONFIRM_YES = {"sim", "s", "ok", "confirmar", "confirmo", "pode"}
ASSISTENCIA_CONFIRM_NO = {"nao", "não", "n", "alterar", "trocar"}
ASSISTENCIA_GREETING_WORDS = {
    "oi",
    "ola",
    "olá",
    "ola tudo bem",
    "olá tudo bem",
    "oi tudo bem",
    "opa",
    "opa tudo bem",
    "e ai",
    "e ai tudo bem",
    "e aí",
    "e aí tudo bem",
    "bom dia",
    "boa tarde",
    "boa noite",
    "seja bem-vindo",
    "seja bem vindo",
}
ASSISTENCIA_GREETING_MESSAGES = (
    "Ola! Como posso ajudar?",
    "Oi! Em que posso ajudar voce hoje?",
    "Ola! Bem-vindo ao atendimento.",
    "Oi! Vamos comecar seu atendimento.",
    "Ola! Sou o assistente da assistencia tecnica.",
    "Oi! Estou aqui para ajudar com seu atendimento.",
    "Ola! Vamos agendar seu atendimento tecnico.",
    "Oi! Vou ajudar voce a abrir um chamado.",
    "Ola! Vamos resolver seu problema.",
    "Oi! Pode me contar o que aconteceu?",
)
ASSISTENCIA_FLOW = [
    "nome",
    "whatsapp",
    "tipo_equipamento",
    "marca",
    "modelo",
    "problema",
    "endereco",
    "dia",
    "horario",
]
ASSISTENCIA_QUESTIONS = {
    "nome": "Ola, tudo bem? Qual e o seu nome?",
    "whatsapp": "Qual e o seu WhatsApp com DDD?",
    "tipo_equipamento": "Qual e o tipo de equipamento? Exemplo: chuveiro, aquecedor a gas, aquecedor eletrico ou pressurizador.",
    "marca": "Qual e a marca do equipamento?",
    "modelo": "Qual e o modelo do equipamento?",
    "problema": "Qual e o problema do equipamento?",
    "endereco": "Qual e o endereco para o atendimento?",
    "dia": "Perfeito.\nQual dia voce estara em casa para receber o tecnico?",
    "horario": "Temos horarios disponiveis:\n09:00\n14:00\n16:00\n\nQual horario prefere?",
}
ASSISTENCIA_DAY_QUESTIONS = (
    "Perfeito.\nQual dia voce estara em casa para receber o tecnico?",
    "Em qual dia podemos enviar o tecnico ate sua casa?",
    "Qual a melhor data para o atendimento?",
    "Que dia voce prefere que o tecnico va ate sua casa?",
    "Qual dia fica melhor para agendarmos a visita?",
    "Quando podemos marcar a visita do tecnico?",
    "Voce pode informar um dia para o atendimento?",
    "Qual data voce estara disponivel?",
    "Em qual dia podemos realizar o atendimento tecnico?",
    "Qual o melhor dia para resolvermos o problema?",
)
ASSISTENCIA_HOUR_QUESTIONS = (
    "Qual horario voce estara em casa?",
    "Em qual periodo voce prefere o atendimento?",
    "Qual horario fica melhor para a visita do tecnico?",
    "Voce prefere atendimento de manha, tarde ou noite?",
    "Que horas podemos enviar o tecnico?",
    "Entre quais horarios voce estara disponivel?",
    "Qual o melhor horario para o atendimento?",
    "O tecnico pode ir em qual periodo do dia?",
    "Voce estara disponivel em que horario?",
    "Pode me informar um horario aproximado?",
)
ASSISTENCIA_DEFAULT_WHATSAPP = "5511999999999"
ASSISTENCIA_ALLOWED_OS_STATUS = {"Agendado", "Em Atendimento", "Finalizado"}
AUTH_RATE_LIMITS = {}
ASSISTENCIA_URGENCIA_REGRAS = [
    {
        "keywords": ("vazando gas", "vazamento de gas", "cheiro de gas", "cheiro forte de gas", "gas vazando"),
        "orientacao": "Atencao: feche o registro de gas, nao acenda luzes e nao use chamas no local. Se o cheiro estiver forte, mantenha o ambiente ventilado e aguarde o tecnico em seguranca.",
    },
    {
        "keywords": ("faisca", "faísca", "fumaca", "fumaça", "curto", "pegando fogo"),
        "orientacao": "Atencao: desligue a energia no disjuntor e nao utilize o equipamento ate a avaliacao tecnica.",
    },
    {
        "keywords": ("vazando agua", "vazamento de agua", "alagando", "estourou"),
        "orientacao": "Atencao: feche o registro de agua do equipamento e evite ligar novamente ate a avaliacao tecnica.",
    },
]
ASSISTENCIA_TRIAGEM = {
    "aquecedor a gas": [
        "O painel ou chama piloto acende normalmente?",
        "Voce percebe cheiro de gas ou algum codigo de erro?",
    ],
    "aquecedor eletrico": [
        "O equipamento liga normalmente ou desarma o disjuntor?",
        "A agua nao aquece ou aquece muito pouco?",
    ],
    "chuveiro": [
        "O chuveiro aquece normalmente ou desarma o disjuntor?",
        "O problema acontece no jato, na resistencia ou na chave de temperatura?",
    ],
    "pressurizador": [
        "O pressurizador faz barulho ao ligar?",
        "A agua perdeu pressao em todos os pontos ou so em um local?",
    ],
}
ASSISTENCIA_TIPO_KEYWORDS = {
    "aquecedor a gas": ("aquecedor a gas", "aquecedor de gas", "aquecedor gás", "aquecedor gas"),
    "aquecedor eletrico": ("aquecedor eletrico", "aquecedor elétrico"),
    "chuveiro": ("chuveiro",),
    "pressurizador": ("pressurizador",),
}
ASSISTENCIA_MARCAS = (
    "lorenzetti", "rinnai", "komeco", "bosch", "deca", "hydra", "fame", "zetta", "cardal"
)
ASSISTENCIA_INTENT_REAGENDAR = (
    "reagendar",
    "remarcar",
    "reagendar chamado",
    "reagendar o chamado",
    "remarcar chamado",
    "remarcar o chamado",
    "reagendar atendimento",
    "remarcar atendimento",
    "reagendar visita",
    "reagendar visita do tecnico",
    "mudar data do chamado",
    "alterar data do chamado",
    "mudar a data do chamado",
    "quero mudar a data do atendimento",
    "mudar data do atendimento",
    "alterar visita do tecnico",
    "mudar agendamento do chamado",
    "trocar o dia do chamado",
    "trocar dia do chamado",
    "mudar horario",
    "mudar horário",
    "trocar horario",
    "trocar horário",
    "mudar horario do chamado",
    "mudar horário do chamado",
    "alterar horario do atendimento",
    "alterar horário do atendimento",
    "mudar horario da visita",
    "mudar horário da visita",
    "pode ser reagendado",
    "nao vou estar em casa no chamado",
    "não vou estar em casa no chamado",
)
ASSISTENCIA_INTENT_CONSULTAR = ("minha os", "minha ordem", "status da os", "status da ordem", "consultar os", "consultar ordem")
ASSISTENCIA_WEEKDAYS = {
    "segunda": 0,
    "segunda-feira": 0,
    "terca": 1,
    "terça": 1,
    "terca-feira": 1,
    "terça-feira": 1,
    "quarta": 2,
    "quarta-feira": 2,
    "quinta": 3,
    "quinta-feira": 3,
    "sexta": 4,
    "sexta-feira": 4,
    "sabado": 5,
    "sábado": 5,
    "domingo": 6,
}
ASSISTENCIA_PERIOD_SLOTS = {
    "manha": ["09:00"],
    "manhã": ["09:00"],
    "de manha": ["09:00"],
    "de manhã": ["09:00"],
    "tarde": ["14:00", "16:00"],
    "de tarde": ["14:00", "16:00"],
    "fim da tarde": ["16:00"],
    "noite": [],
}
ASSISTENCIA_FAQ = {
    "preco_visita": (
        "quanto custa a visita", "preco da visita", "preço da visita",
        "valor da visita", "quanto e a visita", "quanto é a visita"
    ),
    "garantia": ("garantia", "tem garantia", "como funciona a garantia"),
    "pagamento": ("forma de pagamento", "formas de pagamento", "pagamento", "aceita pix", "aceita cartao", "aceita cartão"),
    "atendimento_cidade": ("atende minha cidade", "atende em", "vocês atendem", "voces atendem", "atendimento na minha cidade"),
    "produto": (
        "vendem", "vcs vendem", "vocês vendem", "tem produto", "tem chuveiro",
        "quanto custa", "preco", "preço", "valor", "qual e o valor", "qual é o valor",
        "qual e o preco", "qual é o preco", "qual e o preço", "qual é o preço"
    ),
    "entrega": ("fazem entrega", "faz entrega", "entregam", "entrega de produtos", "integra de produtos", "entrega"),
}
ASSISTENCIA_INTENT_AGENDAR = (
    "agendar", "marcar visita", "marca visita", "quero marcar", "quero marca",
    "quero agendar", "preciso de visita", "quero atendimento", "abrir chamado",
    "abrir uma os", "quero uma visita", "quero uma assistencia", "quero uma assistência",
    "quero assitencia", "quero assistência", "orcamento", "orçamento"
)
ASSISTENCIA_SERVICE_TYPES = {
    "orcamento": ("orcamento", "orçamento", "preco", "preço", "valor"),
    "instalacao": ("instalacao", "instalação", "instalar"),
    "visita_tecnica": (
        "visita tecnica", "visita técnica", "manutencao", "manutenção",
        "conserto", "reparo", "assistencia", "assistência", "assitencia"
    ),
}
ASSISTENCIA_CANCEL_INTENTS = (
    "nao quero agendar", "não quero agendar", "só quero fazer uma pergunta", "so quero fazer uma pergunta",
    "eu so quero fazer uma pergunta", "eu só quero fazer uma pergunta", "cancelar atendimento", "parar atendimento"
)

def get_db():
    conn = sqlite3.connect("database.db", timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def client_ip():
    forwarded = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    return forwarded or (request.remote_addr or "unknown")


def is_same_origin_request():
    origem = request.headers.get("Origin") or request.headers.get("Referer")
    if not origem:
        return False
    origem_url = urlparse(origem)
    destino_url = urlparse(request.host_url)
    return origem_url.scheme == destino_url.scheme and origem_url.netloc == destino_url.netloc


def enforce_auth_rate_limit(scope, identity="", limit=5, window_seconds=300):
    agora = time.time()
    chave = f"{scope}:{client_ip()}:{str(identity or '').strip().lower()[:120]}"
    tentativas = [stamp for stamp in AUTH_RATE_LIMITS.get(chave, []) if agora - stamp < window_seconds]
    AUTH_RATE_LIMITS[chave] = tentativas
    if len(tentativas) >= limit:
        restante = max(1, int(window_seconds - (agora - tentativas[0])))
        return chave, restante
    return chave, 0


def register_auth_failure(chave):
    agora = time.time()
    tentativas = [stamp for stamp in AUTH_RATE_LIMITS.get(chave, []) if agora - stamp < 300]
    tentativas.append(agora)
    AUTH_RATE_LIMITS[chave] = tentativas[-20:]


def clear_auth_failures(chave):
    AUTH_RATE_LIMITS.pop(chave, None)


def get_assistencia_question(field):
    if field == "dia":
        return ASSISTENCIA_DAY_QUESTIONS[secrets.randbelow(len(ASSISTENCIA_DAY_QUESTIONS))]
    if field == "horario":
        return ASSISTENCIA_HOUR_QUESTIONS[secrets.randbelow(len(ASSISTENCIA_HOUR_QUESTIONS))]
    return ASSISTENCIA_QUESTIONS[field]


def get_assistencia_greeting_message():
    return ASSISTENCIA_GREETING_MESSAGES[secrets.randbelow(len(ASSISTENCIA_GREETING_MESSAGES))]


def build_assistencia_horario_prompt(customer_data, horarios_lista, prefixo=""):
    pergunta = get_assistencia_question("horario")
    return (
        f"{prefixo}Temos estes horarios disponiveis para {format_assistencia_day(customer_data.get('dia'))}:\n"
        f"{chr(10).join(horarios_lista)}\n\n{pergunta}"
    )


def get_assistencia_state():
    if ASSISTENCIA_SESSION_KEY not in session:
        session[ASSISTENCIA_SESSION_KEY] = {field: "" for field in ASSISTENCIA_FLOW}
    return session[ASSISTENCIA_SESSION_KEY]


def save_assistencia_state(customer_data):
    session[ASSISTENCIA_SESSION_KEY] = customer_data


def clear_assistencia_state():
    session.pop(ASSISTENCIA_SESSION_KEY, None)


def normalized_assistencia_text(message):
    return " ".join((message or "").strip().lower().split())


def singularize_assistencia_word(word):
    token = str(word or "").strip().lower()
    mapping = {
        "capinhas": "capinha",
        "chuveiros": "chuveiro",
        "aquecedores": "aquecedor",
        "pressurizadores": "pressurizador",
        "produtos": "produto",
        "visitas": "visita",
        "servicos": "servico",
        "serviços": "serviço",
    }
    if token in mapping:
        return mapping[token]
    if token.endswith("oes"):
        return token[:-3] + "ao"
    if token.endswith("ães"):
        return token[:-3] + "ão"
    if token.endswith(("ais", "eis", "ois", "uis")):
        return token
    if token.endswith("s") and len(token) > 4:
        return token[:-1]
    return token


def normalized_assistencia_tokens(message):
    normalized = normalized_assistencia_text(message)
    return [singularize_assistencia_word(part) for part in normalized.split()]


def normalized_assistencia_text_singular(message):
    return " ".join(normalized_assistencia_tokens(message))


def fold_assistencia_text(value):
    texto = str(value or "").strip().lower()
    if not texto:
        return ""
    return unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")


def detect_assistencia_intent(message):
    normalized = normalized_assistencia_text_singular(message)
    folded = fold_assistencia_text(normalized)

    def contains_any(keywords):
        return any(
            keyword in normalized or fold_assistencia_text(keyword) in folded
            for keyword in keywords
        )

    if contains_any(ASSISTENCIA_CANCEL_INTENTS):
        return "cancelar_fluxo"
    if contains_any(ASSISTENCIA_INTENT_AGENDAR):
        return "agendar"
    if contains_any(ASSISTENCIA_INTENT_REAGENDAR):
        return "reagendar"
    if contains_any(ASSISTENCIA_INTENT_CONSULTAR):
        return "consultar_os"
    for intent, keywords in ASSISTENCIA_FAQ.items():
        if contains_any(keywords):
            return intent
    return ""


def detect_service_type(message):
    normalized = normalized_assistencia_text_singular(message)
    for service_type, keywords in ASSISTENCIA_SERVICE_TYPES.items():
        if any(keyword in normalized for keyword in keywords):
            return service_type
    return ""


def is_assistencia_yes(message):
    return normalized_assistencia_text(message) in ASSISTENCIA_CONFIRM_YES


def is_assistencia_no(message):
    return normalized_assistencia_text(message) in ASSISTENCIA_CONFIRM_NO


def is_assistencia_greeting(message):
    normalized = normalized_assistencia_text(message)
    return normalized in ASSISTENCIA_GREETING_WORDS or any(greet in normalized for greet in ASSISTENCIA_GREETING_WORDS)


def extract_assistencia_fields(message):
    raw = str(message or "").strip()
    normalized = normalized_assistencia_text_singular(raw)
    folded = fold_assistencia_text(normalized)
    extracted = {}

    phone_match = re.search(r"(?:\+?55\s*)?(?:\(?\d{2}\)?\s*)?(?:9?\d{4})-?\d{4}", raw)
    if phone_match:
        digits = normalize_phone_digits(phone_match.group(0))
        if len(digits) >= 12:
            extracted["whatsapp"] = digits[-11:]

    name_match = re.search(r"\bmeu nome e\s+([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s]{2,})", raw, flags=re.IGNORECASE)
    if name_match:
        extracted["nome"] = " ".join(name_match.group(1).strip().split())

    for tipo, aliases in ASSISTENCIA_TIPO_KEYWORDS.items():
        if any(alias in normalized for alias in aliases):
            extracted["tipo_equipamento"] = tipo
            break

    chuveiro_problem_patterns = [
        r"\bchuveiro\b.*\b(nao esquenta|nao aquece|nao esquenta mais|so sai agua fria|agua fria|parou de esquentar|parou de aquecer)\b",
        r"\bchuveiro\b.*\b(parou de funcionar no quente|liga mas nao esquenta|liga mas nao aquece)\b",
        r"\bchuveiro\b.*\b(resistencia|resistência)\b.*\b(queimou|estourou)\b",
        r"\b(resistencia|resistência)\b.*\bdo chuveiro\b.*\b(queimou|estourou)\b",
        r"\bqueimou\b.*\b(resistencia|resistência)\b.*\bdo chuveiro\b",
        r"\bestourou\b.*\b(resistencia|resistência)\b.*\bdo chuveiro\b",
        r"\bparou de aquecer a agua\b",
        r"\bnao esta aquecendo a agua\b",
        r"\ba agua do chuveiro esta fria\b",
        r"\ba agua do chuveiro esta gelada\b",
        r"\ba agua esta saindo gelada\b",
        r"\bo chuveiro so sai agua fria\b",
    ]
    if "tipo_equipamento" not in extracted and any(re.search(pattern, folded, flags=re.IGNORECASE) for pattern in chuveiro_problem_patterns):
        extracted["tipo_equipamento"] = "chuveiro"

    aquecedor_gas_problem_patterns = [
        r"\baquecedor\b.*\b(nao esquenta|nao aquece|nao esquenta mais|nao esta esquentando|parou de esquentar|parou de funcionar|nao esta funcionando)\b",
        r"\baquecedor\b.*\b(nao acende|nao liga|desliga sozinho|nao segura a chama)\b",
        r"\b(chama|piloto)\b.*\bnao acende\b",
        r"\bso sai agua fria do aquecedor\b",
        r"\bnao esta saindo agua quente\b",
        r"\ba agua quente acabou\b",
        r"\ba agua nao fica quente\b",
        r"\ba agua fica quente e depois esfria\b",
        r"\ba agua esta saindo fria\b",
        r"\ba agua nao esta aquecendo\b",
        r"\bo aquecedor queimou\b",
        r"\bo aquecedor nao aquece a agua\b",
    ]
    aquecedor_eletrico_problem_patterns = [
        r"\b(resistencia|resistência)\b.*\bdo aquecedor\b",
        r"\baquecedor\b.*\b(resistencia|resistência)\b.*\b(queimou|estourou)\b",
        r"\bqueimou\b.*\b(resistencia|resistência)\b.*\baquecedor\b",
    ]
    if "tipo_equipamento" not in extracted:
        if any(re.search(pattern, folded, flags=re.IGNORECASE) for pattern in aquecedor_eletrico_problem_patterns):
            extracted["tipo_equipamento"] = "aquecedor eletrico"
        elif any(re.search(pattern, folded, flags=re.IGNORECASE) for pattern in aquecedor_gas_problem_patterns):
            extracted["tipo_equipamento"] = "aquecedor a gas"

    marca_match = re.search(r"\bmarca\s+([A-Za-z0-9-]+)", raw, flags=re.IGNORECASE)
    if marca_match:
        extracted["marca"] = marca_match.group(1).strip()
    else:
        for marca in ASSISTENCIA_MARCAS:
            if marca in normalized:
                extracted["marca"] = marca
                break

    modelo_match = re.search(r"\bmodelo\s+([A-Za-z0-9./-]+(?:\s+[A-Za-z0-9./-]+)?)(?=\s+\bque\b|,|$)", raw, flags=re.IGNORECASE)
    if modelo_match:
        extracted["modelo"] = modelo_match.group(1).strip()

    problem_patterns = [
        r"\bnao\s+aquece\b",
        r"\bnão\s+aquece\b",
        r"\bnao\s+esquenta\b",
        r"\bnão\s+esquenta\b",
        r"\bnao\s+esquenta\s+mais\b",
        r"\bnão\s+esquenta\s+mais\b",
        r"\bnao\s+liga\b",
        r"\bnão\s+liga\b",
        r"\bparou\b",
        r"\bparou\s+de\s+esquentar\b",
        r"\bparou\s+de\s+aquecer\b",
        r"\bparou\s+de\s+aquecer\s+a\s+agua\b",
        r"\bparou\s+de\s+funcionar\s+no\s+quente\b",
        r"\bparou\s+de\s+funcionar\b",
        r"\bdesarma\b",
        r"\bdesliga\s+sozinho\b",
        r"\berro\b",
        r"\bproblema\b",
        r"\bnao\s+funciona\b",
        r"\bnão\s+funciona\b",
        r"\bnao\s+esta\s+funcionando\b",
        r"\bnão\s+está\s+funcionando\b",
        r"\bnao\s+esta\s+aquecendo\s+a\s+agua\b",
        r"\bnão\s+está\s+aquecendo\s+a\s+água\b",
        r"\ba\s+agua\s+nao\s+esta\s+aquecendo\b",
        r"\ba\s+água\s+não\s+está\s+aquecendo\b",
        r"\bnao\s+esta\s+esquentando\b",
        r"\bnão\s+está\s+esquentando\b",
        r"\bnao\s+esta\s+saindo\s+agua\s+quente\b",
        r"\bnão\s+está\s+saindo\s+água\s+quente\b",
        r"\ba\s+agua\s+esta\s+saindo\s+fria\b",
        r"\ba\s+água\s+está\s+saindo\s+fria\b",
        r"\ba\s+agua\s+quente\s+acabou\b",
        r"\ba\s+água\s+quente\s+acabou\b",
        r"\ba\s+agua\s+nao\s+fica\s+quente\b",
        r"\ba\s+água\s+não\s+fica\s+quente\b",
        r"\ba\s+agua\s+fica\s+quente\s+e\s+depois\s+esfria\b",
        r"\ba\s+água\s+fica\s+quente\s+e\s+depois\s+esfria\b",
        r"\bfica\s+quente\s+e\s+depois\s+esfria\b",
        r"\bo\s+aquecedor\s+queimou\b",
        r"\bagua\s+fria\b",
        r"\bágua\s+fria\b",
        r"\besta\s+fria\b",
        r"\bestá\s+fria\b",
        r"\bagua\s+gelada\b",
        r"\bágua\s+gelada\b",
        r"\besta\s+saindo\s+gelada\b",
        r"\bestá\s+saindo\s+gelada\b",
        r"\bso\s+sai\s+agua\s+fria\b",
        r"\bsó\s+sai\s+agua\s+fria\b",
        r"\bso\s+sai\s+água\s+fria\b",
        r"\bsó\s+sai\s+água\s+fria\b",
        r"\bliga\s+mas\s+nao\s+esquenta\b",
        r"\bliga\s+mas\s+não\s+esquenta\b",
        r"\bliga\s+mas\s+nao\s+aquece\b",
        r"\bliga\s+mas\s+não\s+aquece\b",
        r"\bnao\s+acende\b",
        r"\bnão\s+acende\b",
        r"\ba\s+chama\s+nao\s+acende\b",
        r"\ba\s+chama\s+não\s+acende\b",
        r"\bnao\s+segura\s+a\s+chama\b",
        r"\bnão\s+segura\s+a\s+chama\b",
        r"\bvaza(?:mento)?\b",
        r"\bbarulho\b",
        r"\bcheiro\b",
        r"\bfumaca\b",
        r"\bfumaça\b",
        r"\bresistencia\b",
        r"\bresistência\b",
        r"\bqueimou\b",
        r"\bestourou\b",
    ]
    matches = [re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in problem_patterns]
    matches = [m for m in matches if m]
    if matches:
        start = min(m.start() for m in matches)
        problema = raw[start:].strip(" ,.-")
        problema = re.sub(r"^(que|porque)\s+", "", problema, flags=re.IGNORECASE)
        if len(problema) >= 6:
            extracted["problema"] = problema

    return extracted


def extract_city_from_message(message):
    raw = str(message or "").strip()
    match = re.search(r"\bem\s+([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s-]{2,})\??$", raw, flags=re.IGNORECASE)
    if match:
        return " ".join(match.group(1).strip().split())
    return ""


def next_assistencia_field(customer_data):
    for field in ASSISTENCIA_FLOW:
        if not str(customer_data.get(field, "") or "").strip():
            return field
    return None


def normalize_phone_digits(value):
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if digits.startswith("55") and len(digits) > 11:
        return digits
    if len(digits) == 11:
        return f"55{digits}"
    return digits


def formatar_endereco_cliente(cliente):
    if not cliente:
        return ""

    partes = [
        str(cliente["endereco"] or "").strip(),
        str(cliente["numero"] or "").strip(),
        str(cliente["complemento"] or "").strip() if "complemento" in cliente.keys() else "",
    ]
    primeira_linha = " ".join(part for part in partes if part)

    complemento = [
        str(cliente["bairro"] or "").strip(),
        str(cliente["cidade"] or "").strip(),
    ]
    segunda_linha = " - ".join(part for part in complemento if part)

    endereco = ", ".join(part for part in [primeira_linha, segunda_linha] if part)
    return endereco.strip(", -")


def montar_endereco_assistencia(customer_data):
    primeira_parte = " ".join(
        part for part in [
            str(customer_data.get("logradouro") or customer_data.get("endereco") or "").strip(),
            str(customer_data.get("numero") or "").strip(),
            str(customer_data.get("complemento") or "").strip(),
        ] if part
    )
    segunda_parte = " - ".join(
        part for part in [
            str(customer_data.get("bairro") or "").strip(),
            str(customer_data.get("cidade") or "").strip(),
        ] if part
    )
    return ", ".join(part for part in [primeira_parte, segunda_parte] if part).strip(", -")


def buscar_cliente_assistencia_por_whatsapp(whatsapp):
    ensure_clientes_complemento_column()
    numero = normalize_phone_digits(whatsapp)
    if len(numero) < 11:
        return None

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, nome, razao_social, whatsapp, telefone, cep, endereco, numero, complemento, bairro, cidade
        FROM clientes
        WHERE REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(whatsapp, ''), '(', ''), ')', ''), '-', ''), ' ', '') LIKE ?
        ORDER BY id DESC
        LIMIT 1
    """, (f"%{numero[-11:]}%",))
    cliente = cursor.fetchone()
    conn.close()
    return cliente


def normalize_cep(value):
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def is_probable_cep(value):
    return len(normalize_cep(value)) == 8


def ensure_clientes_complemento_column():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(clientes)")
    cols = {c["name"] for c in cursor.fetchall()}
    if "complemento" not in cols:
        cursor.execute("ALTER TABLE clientes ADD COLUMN complemento TEXT")
    conn.commit()
    conn.close()


def ensure_clientes_portal_columns():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(clientes)")
    cols = {c["name"] for c in cursor.fetchall()}
    if "complemento" not in cols:
        cursor.execute("ALTER TABLE clientes ADD COLUMN complemento TEXT")
    if "senha_hash" not in cols:
        cursor.execute("ALTER TABLE clientes ADD COLUMN senha_hash TEXT")
    conn.commit()
    conn.close()


def get_portal_cliente_id():
    return session.get(CLIENTE_PORTAL_SESSION_KEY)


def set_portal_cliente_session(cliente_id):
    session[CLIENTE_PORTAL_SESSION_KEY] = int(cliente_id)


def clear_portal_cliente_session():
    session.pop(CLIENTE_PORTAL_SESSION_KEY, None)


def buscar_cliente_portal_por_id(cliente_id):
    if not cliente_id:
        return None
    ensure_clientes_portal_columns()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, nome, telefone, whatsapp, email, cep, endereco, numero, complemento, bairro, cidade, senha_hash
        FROM clientes
        WHERE id = ?
        LIMIT 1
    """, (cliente_id,))
    cliente = cursor.fetchone()
    conn.close()
    return cliente


def buscar_cliente_portal_por_login(login):
    ensure_clientes_portal_columns()
    termo = str(login or "").strip()
    if not termo:
        return None

    conn = get_db()
    cursor = conn.cursor()
    numero = normalize_phone_digits(termo)
    cliente = None
    if len(numero) >= 12:
        alvo = numero[-11:]
        cursor.execute("""
            SELECT id, nome, telefone, whatsapp, email, cep, endereco, numero, complemento, bairro, cidade, senha_hash
            FROM clientes
            WHERE REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(whatsapp, ''), '(', ''), ')', ''), '-', ''), ' ', '') LIKE ?
               OR REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(telefone, ''), '(', ''), ')', ''), '-', ''), ' ', '') LIKE ?
            ORDER BY id DESC
            LIMIT 1
        """, (f"%{alvo}%", f"%{alvo}%"))
        cliente = cursor.fetchone()
    if cliente is None:
        cursor.execute("""
            SELECT id, nome, telefone, whatsapp, email, cep, endereco, numero, complemento, bairro, cidade, senha_hash
            FROM clientes
            WHERE lower(COALESCE(email, '')) = ?
            ORDER BY id DESC
            LIMIT 1
        """, (termo.lower(),))
        cliente = cursor.fetchone()
    conn.close()
    return cliente


def get_portal_cliente():
    return buscar_cliente_portal_por_id(get_portal_cliente_id())


def hydrate_assistencia_from_portal(customer_data=None):
    cliente = get_portal_cliente()
    if not cliente:
        return customer_data if customer_data is not None else get_assistencia_state()

    state = customer_data if customer_data is not None else get_assistencia_state()
    mudou = False

    nome = str(cliente["nome"] or "").strip()
    whatsapp = str(cliente["whatsapp"] or cliente["telefone"] or "").strip()
    if nome and not str(state.get("nome") or "").strip():
        state["nome"] = nome
        mudou = True
    if whatsapp and not str(state.get("whatsapp") or "").strip():
        state["whatsapp"] = whatsapp
        mudou = True
    if cliente["id"] and not state.get("cliente_id"):
        state["cliente_id"] = cliente["id"]
        mudou = True

    endereco = formatar_endereco_cliente(cliente)
    if endereco and not str(state.get("endereco") or "").strip():
        state["endereco"] = endereco
        state["logradouro"] = str(cliente["endereco"] or "").strip()
        state["numero"] = str(cliente["numero"] or "").strip()
        state["complemento"] = str(cliente["complemento"] or "").strip()
        state["bairro"] = str(cliente["bairro"] or "").strip()
        state["cidade"] = str(cliente["cidade"] or "").strip()
        state["cep"] = str(cliente["cep"] or "").strip()
        mudou = True

    if mudou:
        save_assistencia_state(state)
    return state


def mensagem_inicial_assistencia(customer_data):
    if str(customer_data.get("nome") or "").strip():
        proxima = proxima_pergunta_assistencia(customer_data)
        if proxima:
            return f"Ola, {customer_data['nome'].strip()}.\n{proxima}"
    return f"{get_assistencia_greeting_message()}\n{ASSISTENCIA_QUESTIONS['nome']}"


def formatar_status_portal(status):
    valor = str(status or "").strip() or "Aguardando atendimento"
    normalized = fold_assistencia_text(valor)
    if "finalizado" in normalized:
        return {"texto": valor, "classe": "status-finished", "icone": "OK"}
    if "atendimento" in normalized:
        return {"texto": valor, "classe": "status-progress", "icone": "AT"}
    if "agendado" in normalized:
        return {"texto": valor, "classe": "status-scheduled", "icone": "AG"}
    return {"texto": valor, "classe": "status-waiting", "icone": "AGD"}


def montar_historico_chamado_portal(ordem):
    historico = []
    if ordem["data_criacao"]:
        historico.append({
            "titulo": "Chamado aberto",
            "descricao": f"Solicitacao registrada em {ordem['data_criacao']}.",
        })
    if ordem["tecnico_nome"]:
        data_visita = format_assistencia_day(ordem["data_agendamento"]) if ordem["data_agendamento"] else "-"
        hora_visita = ordem["hora"] or "-"
        historico.append({
            "titulo": "Tecnico responsavel",
            "descricao": f"{ordem['tecnico_nome']} - visita {data_visita} as {hora_visita}.",
        })
    historico.append({
        "titulo": "Status atual",
        "descricao": str(ordem["status"] or "Aguardando atendimento"),
    })
    if ordem["defeito_reclamado"]:
        historico.append({
            "titulo": "Problema informado",
            "descricao": ordem["defeito_reclamado"],
        })
    if ordem["diagnostico"]:
        historico.append({
            "titulo": "Diagnostico",
            "descricao": ordem["diagnostico"],
        })
    if ordem["solucao"]:
        historico.append({
            "titulo": "Solucao",
            "descricao": ordem["solucao"],
        })
    return historico


def listar_chamados_cliente_portal(cliente_id):
    if not cliente_id:
        return []
    ensure_ordem_servico_mobile_columns()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            os.id,
            os.servico_nome,
            os.status,
            os.data_agendamento,
            os.hora,
            os.data_criacao,
            os.defeito_reclamado,
            os.observacao,
            os.diagnostico,
            os.solucao,
            f.nome AS tecnico_nome
        FROM ordem_servico os
        LEFT JOIN funcionarios f ON f.id = os.tecnico_id
        WHERE os.cliente_id = ?
        ORDER BY COALESCE(os.data_agendamento, ''), COALESCE(os.hora, ''), os.id DESC
    """, (cliente_id,))
    rows = cursor.fetchall()
    conn.close()

    chamados = []
    for row in rows:
        status_info = formatar_status_portal(row["status"])
        chamados.append({
            "id": row["id"],
            "servico_nome": row["servico_nome"] or "Assistencia Tecnica",
            "status": row["status"] or "Aguardando atendimento",
            "status_classe": status_info["classe"],
            "status_icone": status_info["icone"],
            "tecnico_nome": row["tecnico_nome"] or "A definir",
            "data_visita": format_assistencia_day(row["data_agendamento"]) if row["data_agendamento"] else "A combinar",
            "hora_visita": row["hora"] or "A combinar",
            "historico": montar_historico_chamado_portal(row),
        })
    chamados.reverse()
    return chamados


def atualizar_cliente_portal(cliente_id, nome, telefone, email, endereco_texto):
    parsed = parse_endereco_manual(endereco_texto)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE clientes
        SET nome = ?,
            telefone = ?,
            whatsapp = ?,
            email = ?,
            endereco = ?,
            numero = ?,
            complemento = ?,
            bairro = COALESCE(NULLIF(bairro, ''), ''),
            cidade = COALESCE(NULLIF(cidade, ''), '')
        WHERE id = ?
    """, (
        nome,
        telefone,
        telefone,
        email,
        parsed["logradouro"] if parsed else endereco_texto,
        parsed["numero"] if parsed else "",
        parsed["complemento"] if parsed else "",
        cliente_id,
    ))
    conn.commit()
    conn.close()
    return parsed


def criar_chamado_portal_cliente(cliente, dados):
    ensure_ordem_servico_mobile_columns()
    nome = str(dados.get("nome") or cliente["nome"] or "").strip()
    telefone = normalize_phone_digits(dados.get("telefone") or cliente["whatsapp"] or cliente["telefone"] or "")
    email = str(dados.get("email") or cliente["email"] or "").strip()
    endereco_texto = str(dados.get("endereco") or formatar_endereco_cliente(cliente) or "").strip()
    equipamento = str(dados.get("equipamento") or "").strip()
    marca = str(dados.get("marca") or "").strip()
    modelo = str(dados.get("modelo") or "").strip()
    problema = str(dados.get("problema") or "").strip()
    data_disponivel = str(dados.get("data_disponivel") or "").strip()

    if len(telefone) < 12:
        raise ValueError("Informe um telefone valido com DDD.")
    if not all([nome, endereco_texto, equipamento, problema, data_disponivel]):
        raise ValueError("Preencha nome, telefone, endereco, equipamento, problema e data disponivel.")
    try:
        datetime.strptime(data_disponivel, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Informe uma data valida para a visita.") from exc

    parsed_endereco = atualizar_cliente_portal(cliente["id"], nome, telefone[-11:], email, endereco_texto)
    customer_data = {
        "cliente_id": cliente["id"],
        "nome": nome,
        "whatsapp": telefone[-11:],
        "tipo_equipamento": equipamento,
        "marca": marca,
        "modelo": modelo,
        "problema": problema,
        "dia": data_disponivel,
        "horario": "",
        "service_type": "visita_tecnica",
        "logradouro": parsed_endereco["logradouro"] if parsed_endereco else endereco_texto,
        "numero": parsed_endereco["numero"] if parsed_endereco else "",
        "complemento": parsed_endereco["complemento"] if parsed_endereco else "",
    }

    cliente_atualizado = buscar_cliente_portal_por_id(cliente["id"])
    if cliente_atualizado:
        customer_data["bairro"] = str(cliente_atualizado["bairro"] or "").strip()
        customer_data["cidade"] = str(cliente_atualizado["cidade"] or "").strip()
        customer_data["cep"] = str(cliente_atualizado["cep"] or "").strip()

    tecnico = assigned_technician_assistencia(customer_data)
    tecnico_id = tecnico["id"] if tecnico else None
    tecnico_nome = tecnico["nome"] if tecnico else "A definir"
    produto_defeito = " ".join(part for part in [equipamento, marca, modelo] if part).strip()
    observacao = (
        "Origem: portal do cliente.\n"
        f"WhatsApp do cliente: {telefone[-11:]}\n"
        f"Endereco informado: {montar_endereco_assistencia(customer_data) or endereco_texto}"
    )

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO ordem_servico (
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """, (
        "Assistencia Tecnica",
        data_disponivel,
        "",
        "Aguardando atendimento",
        cliente["id"],
        tecnico_id,
        problema,
        produto_defeito,
        observacao,
    ))
    chamado_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return chamado_id, tecnico_nome


def buscar_endereco_por_cep(cep):
    cep_limpo = normalize_cep(cep)
    if len(cep_limpo) != 8:
        return None

    url = f"https://viacep.com.br/ws/{cep_limpo}/json/"
    try:
        with urllib_request.urlopen(url, timeout=4) as response:
            data = json.loads(response.read().decode("utf-8"))
    except (urllib_error.URLError, TimeoutError, ValueError, OSError):
        return None

    if data.get("erro"):
        return None

    return {
        "cep": cep_limpo,
        "logradouro": str(data.get("logradouro") or "").strip(),
        "bairro": str(data.get("bairro") or "").strip(),
        "cidade": str(data.get("localidade") or "").strip(),
        "uf": str(data.get("uf") or "").strip(),
    }


def formatar_endereco_cep(endereco):
    if not endereco:
        return ""

    primeira_parte = ", ".join(
        part for part in [
            str(endereco.get("logradouro") or "").strip(),
            str(endereco.get("bairro") or "").strip(),
        ] if part
    )
    segunda_parte = " - ".join(
        part for part in [
            str(endereco.get("cidade") or "").strip(),
            str(endereco.get("uf") or "").strip(),
        ] if part
    )
    return ", ".join(part for part in [primeira_parte, segunda_parte] if part)


def parse_endereco_manual(texto):
    raw = str(texto or "").strip()
    if not raw:
        return None

    normalized = " ".join(raw.replace(";", ",").split())
    normalized_lower = normalized.lower()

    sem_numero_match = re.match(
        r"^(.*?)(?:[,\s]+)(s/?n|sem numero|sem número)(?:[,\s]+(.*))?$",
        normalized_lower,
        flags=re.IGNORECASE,
    )
    if sem_numero_match:
        prefix_len = len(sem_numero_match.group(1) or "")
        suffix = sem_numero_match.group(3) or ""
        return {
            "logradouro": normalized[:prefix_len].strip(" ,"),
            "numero": "s/n",
            "complemento": suffix.strip(" ,"),
        }

    complemento_match = re.match(
        r"^(.*?)[,\s]+(\d+[A-Za-z0-9/-]*)(?:[,\s]+(apto|apartamento|bloco|casa|fundos|sala|loja|sobrado|torre)\b(.*))?$",
        normalized,
        flags=re.IGNORECASE,
    )
    if complemento_match:
        logradouro = (complemento_match.group(1) or "").strip(" ,")
        numero = (complemento_match.group(2) or "").strip(" ,")
        complemento_prefixo = (complemento_match.group(3) or "").strip(" ,")
        complemento_sufixo = (complemento_match.group(4) or "").strip(" ,")
        complemento = " ".join(part for part in [complemento_prefixo, complemento_sufixo] if part).strip()
        if logradouro:
            return {
                "logradouro": logradouro,
                "numero": numero,
                "complemento": complemento,
            }

    match = re.match(r"^(.*?)[,\s]+(\d+[A-Za-z0-9/-]*)(?:[,\s]+(.*))?$", normalized)
    if not match:
        return {
            "logradouro": normalized,
            "numero": "",
            "complemento": "",
        }

    logradouro = (match.group(1) or "").strip(" ,")
    numero = (match.group(2) or "").strip(" ,")
    complemento = (match.group(3) or "").strip(" ,")

    if not logradouro:
        return {
            "logradouro": normalized,
            "numero": "",
            "complemento": "",
        }

    return {
        "logradouro": logradouro,
        "numero": numero,
        "complemento": complemento,
    }


def descricao_disponibilidade_horarios(data_agendamento):
    horarios = horarios_disponiveis_assistencia(data_agendamento)
    if horarios:
        return "\n".join(horarios)
    return "Nenhum horario disponivel"


def filtrar_horarios_preferidos(horarios, preferred_slots):
    if not preferred_slots:
        return list(horarios)
    filtrados = [slot for slot in horarios if slot in preferred_slots]
    return filtrados or list(horarios)


def tecnico_disponivel_assistencia(customer_data):
    tecnicos = listar_tecnicos_assistencia()
    if not tecnicos:
        return None

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT tecnico_id
        FROM ordem_servico
        WHERE data_agendamento = ?
          AND hora = ?
          AND tecnico_id IS NOT NULL
          AND COALESCE(status, '') NOT IN ('Finalizado', 'Cancelado')
    """, (
        customer_data.get("dia"),
        customer_data.get("horario"),
    ))
    ocupados = {row["tecnico_id"] for row in cursor.fetchall()}
    conn.close()

    disponiveis = [tecnico for tecnico in tecnicos if tecnico["id"] not in ocupados]
    if not disponiveis:
        return None

    seed_base = (
        str(customer_data.get("nome", ""))
        + str(customer_data.get("marca", ""))
        + str(customer_data.get("modelo", ""))
    )
    seed = sum(ord(char) for char in seed_base)
    return disponiveis[seed % len(disponiveis)]


def horarios_disponiveis_assistencia(data_agendamento):
    if not data_agendamento:
        return list(ASSISTENCIA_SLOTS)

    tecnicos = listar_tecnicos_assistencia()
    if not tecnicos:
        return list(ASSISTENCIA_SLOTS)

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT hora, COUNT(DISTINCT tecnico_id) AS total
        FROM ordem_servico
        WHERE data_agendamento = ?
          AND tecnico_id IS NOT NULL
          AND COALESCE(status, '') NOT IN ('Finalizado', 'Cancelado')
        GROUP BY hora
    """, (data_agendamento,))
    ocupacao = {row["hora"]: int(row["total"] or 0) for row in cursor.fetchall()}
    conn.close()

    capacidade = len(tecnicos)
    return [slot for slot in ASSISTENCIA_SLOTS if ocupacao.get(slot, 0) < capacidade]


def reset_assistencia_confirmation(customer_data):
    customer_data.pop("pending_action", None)
    customer_data.pop("tecnico_sugerido_id", None)
    customer_data.pop("tecnico_sugerido_nome", None)


def detectar_urgencia_assistencia(texto):
    normalized = normalized_assistencia_text(texto)
    for regra in ASSISTENCIA_URGENCIA_REGRAS:
        if any(keyword in normalized for keyword in regra["keywords"]):
            return regra["orientacao"]
    return None


def perguntas_triagem_assistencia(tipo_equipamento):
    tipo = normalized_assistencia_text(tipo_equipamento)
    for chave, perguntas in ASSISTENCIA_TRIAGEM.items():
        if chave in tipo or tipo in chave:
            return list(perguntas)
    return []


def iniciar_triagem_se_necessario(customer_data):
    if not str(customer_data.get("problema") or "").strip():
        return ""
    if customer_data.get("pending_action") == "triagem_tecnica":
        perguntas = customer_data.get("triagem_perguntas") or []
        return perguntas[0] if perguntas else ""
    if customer_data.get("triagem_respostas"):
        return ""

    urgencia = detectar_urgencia_assistencia(customer_data.get("problema"))
    if urgencia:
        customer_data["alerta_urgencia"] = urgencia

    perguntas = perguntas_triagem_assistencia(customer_data.get("tipo_equipamento"))
    if not perguntas:
        return f"{urgencia}\n\n" if urgencia else ""

    customer_data["triagem_perguntas"] = perguntas
    customer_data["triagem_respostas"] = []
    customer_data["triagem_indice"] = 0
    customer_data["pending_action"] = "triagem_tecnica"
    prefixo = f"{urgencia}\n\n" if urgencia else ""
    return f"{prefixo}{perguntas[0]}"


def buscar_os_aberta_por_whatsapp(whatsapp):
    numero = normalize_phone_digits(whatsapp)
    if len(numero) < 11:
        return None

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            os.id,
            os.status,
            os.data_agendamento,
            os.hora,
            os.servico_nome,
            c.id AS cliente_id,
            c.nome,
            c.whatsapp
        FROM ordem_servico os
        LEFT JOIN clientes c ON c.id = os.cliente_id
        WHERE REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(c.whatsapp, ''), '(', ''), ')', ''), '-', ''), ' ', '') LIKE ?
          AND COALESCE(os.status, '') NOT IN ('Finalizado', 'Cancelado')
        ORDER BY os.id DESC
        LIMIT 1
    """, (f"%{numero[-11:]}%",))
    os_row = cursor.fetchone()
    conn.close()
    return os_row


def buscar_os_aberta_por_cliente(cliente_id):
    if not cliente_id:
        return None

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            os.id,
            os.status,
            os.data_agendamento,
            os.hora,
            os.servico_nome,
            c.id AS cliente_id,
            c.nome,
            c.whatsapp
        FROM ordem_servico os
        LEFT JOIN clientes c ON c.id = os.cliente_id
        WHERE os.cliente_id = ?
          AND COALESCE(os.status, '') NOT IN ('Finalizado', 'Cancelado')
        ORDER BY os.id DESC
        LIMIT 1
    """, (cliente_id,))
    os_row = cursor.fetchone()
    conn.close()
    return os_row


def resumo_os_existente(os_row):
    return (
        f"Encontrei a OS #{os_row['id']} com status {os_row['status']}.\n"
        f"Data atual: {format_assistencia_day(os_row['data_agendamento'])} as {os_row['hora'] or '-'}."
    )


def get_loja_config():
    ensure_loja_config_table()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM loja_config ORDER BY id LIMIT 1")
    loja = cursor.fetchone()
    conn.close()
    return loja


def buscar_produto_assistencia(message):
    termo = limpar_consulta_produto(message)
    if not termo:
        return []
    termo_fold = fold_assistencia_text(termo)

    codigo_match = re.search(r"\b([A-Za-z]{2,}\d{2,}|\d{3,})\b", termo)
    codigo = codigo_match.group(1) if codigo_match else ""

    conn = get_db()
    cursor = conn.cursor()
    if codigo:
        cursor.execute("""
            SELECT sku, codigo_fabricante, nome, preco_venda
            FROM produtos
            WHERE UPPER(COALESCE(codigo_fabricante, '')) = UPPER(?)
               OR UPPER(COALESCE(sku, '')) = UPPER(?)
            LIMIT 3
        """, (codigo, codigo))
        produtos = cursor.fetchall()
        if produtos:
            conn.close()
            return produtos

    cursor.execute("""
        SELECT sku, codigo_fabricante, nome, preco_venda
        FROM produtos
        ORDER BY nome
        LIMIT 200
    """)
    produtos = cursor.fetchall()
    conn.close()

    correspondencias = []
    for produto in produtos:
        alvo = " ".join(
            str(produto[campo] or "")
            for campo in ("nome", "codigo_fabricante", "sku")
        )
        alvo_fold = fold_assistencia_text(alvo)
        if termo_fold and termo_fold in alvo_fold:
            correspondencias.append(produto)

    return correspondencias[:5]


def limpar_consulta_produto(message):
    texto = str(message or "").strip()
    if not texto:
        return ""
    normalized = normalized_assistencia_text_singular(texto)
    remover = [
        "quanto custa", "qual o preco", "qual o preço", "qual e o valor", "qual é o valor",
        "qual e o preco", "qual é o preco", "qual e o preço", "qual é o preço", "valor", "preco", "preço",
        "vcs vendem", "vocês vendem", "vendem", "tem", "produto", "quero saber o", "quero saber",
    ]
    limpo = normalized
    for trecho in remover:
        limpo = limpo.replace(trecho, " ")
    limpo = re.sub(r"[?,.-]+", " ", limpo)
    limpo = " ".join(limpo.split())
    return limpo


def responder_faq_assistencia(intent, message, customer_data=None):
    loja = get_loja_config()
    loja_cidade = str(loja["cidade"] or "").strip() if loja else ""
    loja_endereco = " ".join(
        part for part in [
            str(loja["endereco"] or "").strip() if loja else "",
            str(loja["numero"] or "").strip() if loja else "",
            str(loja["bairro"] or "").strip() if loja else "",
            loja_cidade,
        ] if part
    ).strip()
    cidade_citada = extract_city_from_message(message) or str((customer_data or {}).get("cidade_interesse") or "").strip()

    if intent == "preco_visita":
        tipo = str((customer_data or {}).get("tipo_equipamento") or "").strip()
        detalhe = f" para {tipo}" if tipo else ""
        cidade_msg = f" em {cidade_citada}" if cidade_citada else ""
        return (
            f"O valor da visita tecnica{detalhe}{cidade_msg} pode variar conforme o tipo de equipamento e a regiao atendida. "
            "Se quiser, eu sigo com o agendamento e sua equipe pode confirmar o valor antes da visita."
        )

    if intent == "garantia":
        return (
            "A garantia depende do servico executado e das pecas aplicadas. "
            "Depois do atendimento, sua equipe pode informar o prazo exato conforme a OS."
        )

    if intent == "pagamento":
        return "As formas de pagamento normalmente aceitas no sistema sao PIX, cartao, dinheiro, transferencia e boleto."

    if intent == "atendimento_cidade":
        if loja_cidade:
            base = f"A base da loja fica em {loja_cidade}"
            if loja_endereco:
                base += f" ({loja_endereco})"
            if cidade_citada:
                return f"{base}. Se quiser atendimento em {cidade_citada}, posso seguir com o agendamento e sua equipe confirma a cobertura."
            return f"{base}. Se quiser, me diga sua cidade e eu sigo com o atendimento para sua equipe confirmar a cobertura."
        return "Posso seguir com o atendimento e sua equipe confirma a cobertura da sua cidade antes da visita."

    if intent == "produto":
        produtos = buscar_produto_assistencia(message)
        if produtos:
            if len(produtos) == 1:
                produto = produtos[0]
                referencia = produto["codigo_fabricante"] or produto["sku"] or produto["nome"]
                return f"Encontrei o produto {produto['nome'].strip()} ({referencia}) por R$ {float(produto['preco_venda'] or 0):.2f}."
            linhas = ["Encontrei estas opcoes:"]
            for produto in produtos[:3]:
                referencia = produto["codigo_fabricante"] or produto["sku"] or produto["nome"]
                linhas.append(f"- {produto['nome'].strip()} ({referencia}) - R$ {float(produto['preco_venda'] or 0):.2f}")
            linhas.append("Se quiser, me diga o codigo ou nome exato da opcao.")
            return "\n".join(linhas)
        return "Nao encontrei esse produto no cadastro agora. Se quiser, me envie o codigo, sku ou nome do item."

    if intent == "entrega":
        return "Posso verificar a entrega de produtos, mas a confirmacao depende da regiao e da disponibilidade da loja. Se quiser, me diga sua cidade ou o produto."

    return ""


def clear_assistencia_from_field(customer_data, field):
    if field in ASSISTENCIA_FLOW:
        idx = ASSISTENCIA_FLOW.index(field)
        for flow_field in ASSISTENCIA_FLOW[idx:]:
            customer_data[flow_field] = ""

    if field == "whatsapp":
        customer_data.pop("cliente_id", None)
        customer_data.pop("usar_endereco_cadastrado", None)
    if field == "problema":
        customer_data.pop("triagem_perguntas", None)
        customer_data.pop("triagem_respostas", None)
        customer_data.pop("triagem_indice", None)
        customer_data.pop("alerta_urgencia", None)
    if field == "endereco":
        customer_data["endereco"] = ""
        customer_data["cep"] = ""
        customer_data["logradouro"] = ""
        customer_data["numero"] = ""
        customer_data["complemento"] = ""
        customer_data["bairro"] = ""
        customer_data["cidade"] = ""
        customer_data.pop("endereco_base", None)
        customer_data.pop("cliente_id", None)
        customer_data.pop("usar_endereco_cadastrado", None)
    if field == "dia":
        customer_data["horario"] = ""
        customer_data.pop("preferred_slots", None)

    reset_assistencia_confirmation(customer_data)


def map_assistencia_edit_field(message):
    normalized = normalized_assistencia_text(message)
    aliases = {
        "nome": "nome",
        "whatsapp": "whatsapp",
        "telefone": "whatsapp",
        "equipamento": "tipo_equipamento",
        "tipo equipamento": "tipo_equipamento",
        "tipo de equipamento": "tipo_equipamento",
        "marca": "marca",
        "modelo": "modelo",
        "problema": "problema",
        "defeito": "problema",
        "endereco": "endereco",
        "endereço": "endereco",
        "dia": "dia",
        "data": "dia",
        "horario": "horario",
        "horário": "horario",
    }
    return aliases.get(normalized)


def resumo_assistencia(customer_data):
    tecnico_nome = customer_data.get("tecnico_sugerido_nome") or "da equipe"
    endereco = montar_endereco_assistencia(customer_data) or customer_data.get("endereco", "")
    service_type = customer_data.get("service_type") or "visita_tecnica"
    service_label = {
        "orcamento": "Orcamento",
        "instalacao": "Instalacao",
        "visita_tecnica": "Visita tecnica",
    }.get(service_type, "Visita tecnica")
    triagem_respostas = customer_data.get("triagem_respostas") or []
    linhas_triagem = ""
    if triagem_respostas:
        linhas_triagem = "Triagem:\n" + "\n".join(
            f"- {item['pergunta']} {item['resposta']}" for item in triagem_respostas
        ) + "\n"
    alerta = ""
    if customer_data.get("alerta_urgencia"):
        alerta = f"Alerta: {customer_data['alerta_urgencia']}\n"
    return (
        "Confira os dados do agendamento:\n"
        f"Tipo de atendimento: {service_label}\n"
        f"Cliente: {customer_data.get('nome', '').strip()}\n"
        f"WhatsApp: {customer_data.get('whatsapp', '').strip()}\n"
        f"Equipamento: {customer_data.get('tipo_equipamento', '').strip()}\n"
        f"Marca: {customer_data.get('marca', '').strip()}\n"
        f"Modelo: {customer_data.get('modelo', '').strip()}\n"
        f"Problema: {customer_data.get('problema', '').strip()}\n"
        f"{alerta}"
        f"{linhas_triagem}"
        f"Endereco: {endereco}\n"
        f"Data: {format_assistencia_day(customer_data.get('dia'))} as {customer_data.get('horario', '').strip()}\n"
        f"Tecnico previsto: {tecnico_nome}\n\n"
        "Responda 'sim' para confirmar ou 'nao' para alterar algum dado."
    )


def proxima_pergunta_assistencia(customer_data, resposta_prefixo=""):
    upcoming_field = next_assistencia_field(customer_data)
    if not upcoming_field:
        return ""
    if upcoming_field == "endereco":
        return f"{resposta_prefixo}{customer_data.get('nome', '').strip()}, qual e o endereco para o atendimento? Voce pode informar o endereco completo ou enviar o CEP."
    if upcoming_field == "horario":
        horarios_lista = filtrar_horarios_preferidos(
            horarios_disponiveis_assistencia(customer_data.get("dia")),
            customer_data.get("preferred_slots"),
        )
        if not horarios_lista:
            return f"{resposta_prefixo}Nao ha horarios livres nessa data. Escolha outro dia para o atendimento."
        return build_assistencia_horario_prompt(customer_data, horarios_lista, resposta_prefixo)
    if upcoming_field == "problema":
        service_type = customer_data.get("service_type")
        if service_type == "instalacao":
            return f"{resposta_prefixo}O que voce precisa instalar e qual e a necessidade no local?"
        if service_type == "orcamento":
            return f"{resposta_prefixo}Descreva rapidamente o que voce precisa para eu registrar o pedido de orcamento."
    return f"{resposta_prefixo}{get_assistencia_question(upcoming_field)}"


def should_autostart_booking(customer_data, message, extracted):
    normalized = normalized_assistencia_text(message)
    if customer_data.get("intent") == "agendar":
        return True
    if any(keyword in normalized for keyword in ASSISTENCIA_INTENT_AGENDAR):
        return True
    if extracted.get("whatsapp") and extracted.get("tipo_equipamento"):
        return True
    if extracted.get("problema") and extracted.get("tipo_equipamento"):
        return True
    return False


def should_treat_as_product_query(customer_data, message):
    normalized = normalized_assistencia_text(message)
    if not normalized or is_assistencia_greeting(message):
        return False
    if normalize_phone_digits(message) and len(normalize_phone_digits(message)) >= 11:
        return False
    if customer_data.get("intent") in {"agendar", "reagendar", "consultar_os"}:
        return False
    if any(str(customer_data.get(field) or "").strip() for field in ("nome", "whatsapp", "tipo_equipamento", "problema")):
        return False
    return bool(buscar_produto_assistencia(message))


def parse_assistencia_day(text):
    raw = str(text or "").strip()
    normalized = " ".join(raw.lower().split())
    today = datetime.now().date()

    if normalized in {"amanha", "amanhã"}:
        return (today + timedelta(days=1)).strftime("%Y-%m-%d")
    if normalized == "hoje":
        return today.strftime("%Y-%m-%d")

    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    for sep in ("/", "-"):
        parts = raw.split(sep)
        if len(parts) == 2 and all(part.isdigit() for part in parts):
            try:
                day = int(parts[0])
                month = int(parts[1])
                parsed = datetime(today.year, month, day).date()
                if parsed < today:
                    parsed = datetime(today.year + 1, month, day).date()
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                pass

    return raw


def suggested_slots_from_text(text):
    normalized = normalized_assistencia_text(text)
    for period, slots in ASSISTENCIA_PERIOD_SLOTS.items():
        if re.search(rf"\b{re.escape(period)}\b", normalized):
            return list(slots)
    return []


def parse_assistencia_schedule(text):
    raw = str(text or "").strip()
    normalized = normalized_assistencia_text(raw)
    today = datetime.now().date()
    preferred_slots = suggested_slots_from_text(raw)
    normalized_sem_periodo = normalized
    for period in sorted(ASSISTENCIA_PERIOD_SLOTS, key=len, reverse=True):
        normalized_sem_periodo = re.sub(rf"\b{re.escape(period)}\b", " ", normalized_sem_periodo)
    normalized_sem_periodo = " ".join(normalized_sem_periodo.split())
    normalized_sem_periodo = re.sub(r"\bde\b$", "", normalized_sem_periodo).strip()

    if "semana que vem" in normalized_sem_periodo:
        base = today + timedelta(days=7)
        for nome, weekday in ASSISTENCIA_WEEKDAYS.items():
            if re.search(rf"\b{re.escape(nome)}\b", normalized_sem_periodo):
                days_ahead = (weekday - base.weekday()) % 7
                return {
                    "day": (base + timedelta(days=days_ahead)).strftime("%Y-%m-%d"),
                    "preferred_slots": preferred_slots,
                }

    for nome, weekday in ASSISTENCIA_WEEKDAYS.items():
        if re.search(rf"\b{re.escape(nome)}\b", normalized_sem_periodo):
            days_ahead = (weekday - today.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            return {
                "day": (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d"),
                "preferred_slots": preferred_slots,
            }

    parsed_day = parse_assistencia_day(normalized_sem_periodo or raw)
    return {
        "day": parsed_day,
        "preferred_slots": preferred_slots,
    }


def parse_assistencia_slot_choice(text, available_slots=None):
    normalized = normalized_assistencia_text(text)
    available = list(available_slots or ASSISTENCIA_SLOTS)

    if text in ASSISTENCIA_SLOTS and text in available:
        return text

    preferred = suggested_slots_from_text(text)
    if preferred:
        for slot in preferred:
            if slot in available:
                return slot

    if "depois das 14" in normalized or "apos as 14" in normalized or "após as 14" in normalized:
        for slot in available:
            if slot >= "14:00":
                return slot

    if "mais cedo" in normalized or "primeiro horario" in normalized or "primeiro horário" in normalized:
        return available[0] if available else ""

    return ""


def format_assistencia_day(value):
    raw = str(value or "").strip()
    if not raw:
        return "-"

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%d/%m/%Y")
        except ValueError:
            pass
    return raw


def get_loja_whatsapp():
    ensure_loja_config_table()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT telefone FROM loja_config ORDER BY id LIMIT 1")
    loja = cursor.fetchone()
    conn.close()
    return normalize_phone_digits(loja["telefone"] if loja and loja["telefone"] else ASSISTENCIA_DEFAULT_WHATSAPP)


def listar_tecnicos_assistencia():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, nome
        FROM funcionarios
        WHERE COALESCE(status, 'Ativo') = 'Ativo'
          AND (
              LOWER(COALESCE(cargo, '')) LIKE '%tecnico%'
              OR LOWER(COALESCE(funcao, '')) LIKE '%tecnico%'
              OR LOWER(COALESCE(funcao, '')) LIKE '%manut%'
              OR LOWER(COALESCE(funcao, '')) LIKE '%instal%'
          )
        ORDER BY nome
    """)
    tecnicos = cursor.fetchall()

    if not tecnicos:
        cursor.execute("""
            SELECT id, nome
            FROM funcionarios
            WHERE COALESCE(status, 'Ativo') = 'Ativo'
            ORDER BY nome
        """)
        tecnicos = cursor.fetchall()

    conn.close()
    return tecnicos


def assigned_technician_assistencia(customer_data):
    tecnico = tecnico_disponivel_assistencia(customer_data)
    if tecnico:
        return tecnico

    tecnicos = listar_tecnicos_assistencia()
    if not tecnicos:
        return None

    seed_base = (
        str(customer_data.get("nome", ""))
        + str(customer_data.get("marca", ""))
        + str(customer_data.get("modelo", ""))
    )
    seed = sum(ord(char) for char in seed_base)
    return tecnicos[seed % len(tecnicos)]


def localizar_ou_criar_cliente_assistencia(customer_data):
    ensure_clientes_complemento_column()
    whatsapp = normalize_phone_digits(customer_data.get("whatsapp"))
    cliente = buscar_cliente_assistencia_por_whatsapp(whatsapp)
    if cliente:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE clientes
            SET nome = ?,
                telefone = ?,
                whatsapp = ?,
                cep = ?,
                endereco = ?,
                numero = ?,
                complemento = ?,
                bairro = ?,
                cidade = ?
            WHERE id = ?
        """, (
            customer_data.get("nome", "").strip() or cliente["nome"],
            whatsapp[-11:] if whatsapp else "",
            whatsapp[-11:] if whatsapp else "",
            normalize_cep(customer_data.get("cep")),
            str(customer_data.get("logradouro") or customer_data.get("endereco") or "").strip(),
            str(customer_data.get("numero") or "").strip(),
            str(customer_data.get("complemento") or "").strip(),
            str(customer_data.get("bairro") or "").strip(),
            str(customer_data.get("cidade") or "").strip(),
            cliente["id"],
        ))
        conn.commit()
        conn.close()
        return cliente["id"]

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO clientes (
            tipo, nome, telefone, whatsapp, cep, endereco, numero, complemento, bairro, cidade,
            observacoes, data_cadastro
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "PF",
        customer_data.get("nome", "").strip(),
        whatsapp[-11:] if whatsapp else "",
        whatsapp[-11:] if whatsapp else "",
        normalize_cep(customer_data.get("cep")),
        str(customer_data.get("logradouro") or customer_data.get("endereco") or "").strip(),
        str(customer_data.get("numero") or "").strip(),
        str(customer_data.get("complemento") or "").strip(),
        str(customer_data.get("bairro") or "").strip(),
        str(customer_data.get("cidade") or "").strip(),
        "Cadastro criado pelo atendimento tecnico online",
        datetime.now().strftime("%Y-%m-%d")
    ))

    conn.commit()
    cliente_id = cursor.lastrowid
    conn.close()
    return cliente_id


def criar_os_assistencia(customer_data):
    ensure_ordem_servico_mobile_columns()
    cliente_id = localizar_ou_criar_cliente_assistencia(customer_data)
    tecnico = None
    tecnico_sugerido_id = customer_data.get("tecnico_sugerido_id")
    if tecnico_sugerido_id:
        for item in listar_tecnicos_assistencia():
            if item["id"] == tecnico_sugerido_id:
                tecnico = item
                break
    if tecnico is None:
        tecnico = assigned_technician_assistencia(customer_data)
    tecnico_id = tecnico["id"] if tecnico else None
    endereco_atendimento = montar_endereco_assistencia(customer_data)
    service_type = customer_data.get("service_type") or "visita_tecnica"
    servico_nome = {
        "orcamento": "Orcamento Tecnico",
        "instalacao": "Instalacao Tecnica",
        "visita_tecnica": "Assistencia Tecnica",
    }.get(service_type, "Assistencia Tecnica")
    produto_defeito = " ".join(
        part for part in [
            customer_data.get("tipo_equipamento", "").strip(),
            customer_data.get("marca", "").strip(),
            customer_data.get("modelo", "").strip(),
        ] if part
    )
    observacao = (
        "Origem: atendimento tecnico online.\n"
        f"Tipo de atendimento: {service_type}\n"
        f"WhatsApp do cliente: {customer_data.get('whatsapp', '').strip()}\n"
        f"Endereco informado: {endereco_atendimento}"
    )
    if customer_data.get("alerta_urgencia"):
        observacao += f"\nAlerta de seguranca: {customer_data['alerta_urgencia']}"
    triagem_respostas = customer_data.get("triagem_respostas") or []
    if triagem_respostas:
        observacao += "\nTriagem tecnica:"
        for item in triagem_respostas:
            observacao += f"\n- {item['pergunta']} {item['resposta']}"

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO ordem_servico (
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
        ) VALUES (?, ?, ?, 'Agendado', ?, ?, ?, ?, ?, datetime('now'))
    """, (
        servico_nome,
        customer_data.get("dia"),
        customer_data.get("horario"),
        cliente_id,
        tecnico_id,
        customer_data.get("problema", "").strip(),
        produto_defeito,
        observacao
    ))
    os_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return os_id, tecnico


def carregar_itens_pedido(cursor, movimento_id):
    cursor.execute("""
        SELECT
            COALESCE(p.nome, s.nome, 'Item') AS nome,
            im.quantidade,
            im.preco_unitario,
            im.subtotal
        FROM itens_movimento im
        LEFT JOIN produtos p ON im.produto_id = p.id
        LEFT JOIN servicos s ON s.id = im.produto_id AND p.id IS NULL
        WHERE im.movimento_id = ?
        ORDER BY im.id
    """, (movimento_id,))
    itens = cursor.fetchall()

    if itens:
        return itens

    cursor.execute("""
        SELECT
            ios.descricao AS nome,
            ios.quantidade,
            ios.valor AS preco_unitario,
            ios.subtotal
        FROM ordem_servico os
        JOIN itens_ordem_servico ios ON ios.os_id = os.id
        WHERE os.movimento_id = ?
        ORDER BY ios.id
    """, (movimento_id,))
    return cursor.fetchall()


def normalizar_pdf_texto(valor):
    texto = str(valor or "").strip()
    if not texto:
        return ""
    return unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")


def escapar_pdf_texto(valor):
    return normalizar_pdf_texto(valor).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def gerar_pdf_texto_simples(linhas, titulo="documento"):
    page_width = 595
    page_height = 842
    margin_x = 36
    margin_top = 40
    margin_bottom = 40
    font_size = 9
    leading = 11
    max_linhas = max(1, int((page_height - margin_top - margin_bottom) / leading))

    paginas = [linhas[i:i + max_linhas] for i in range(0, len(linhas), max_linhas)] or [[""]]
    objetos = []

    def adicionar_objeto(conteudo):
        if isinstance(conteudo, str):
            conteudo = conteudo.encode("latin-1")
        objetos.append(conteudo)
        return len(objetos)

    fonte_id = adicionar_objeto("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    conteudos_paginas = []

    for pagina in paginas:
        comandos = [
            "BT",
            f"/F1 {font_size} Tf",
            f"{leading} TL",
            f"{margin_x} {page_height - margin_top} Td",
        ]
        for idx, linha in enumerate(pagina):
            if idx > 0:
                comandos.append("T*")
            comandos.append(f"({escapar_pdf_texto(linha)}) Tj")
        comandos.append("ET")
        stream = "\n".join(comandos).encode("latin-1")
        conteudo_id = adicionar_objeto(
            b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream"
        )
        conteudos_paginas.append(conteudo_id)

    paginas_ids = [adicionar_objeto(b"") for _ in conteudos_paginas]
    arvore_paginas_id = adicionar_objeto(
        "<< /Type /Pages /Count {count} /Kids [{kids}] >>".format(
            count=len(paginas_ids),
            kids=" ".join(f"{page_id} 0 R" for page_id in paginas_ids)
        )
    )

    for page_id, conteudo_id in zip(paginas_ids, conteudos_paginas):
        objetos[page_id - 1] = (
            "<< /Type /Page /Parent {parent} 0 R /MediaBox [0 0 {w} {h}] "
            "/Resources << /Font << /F1 {font} 0 R >> >> /Contents {content} 0 R >>"
        ).format(
            parent=arvore_paginas_id,
            w=page_width,
            h=page_height,
            font=fonte_id,
            content=conteudo_id
        ).encode("latin-1")

    catalogo_id = adicionar_objeto(f"<< /Type /Catalog /Pages {arvore_paginas_id} 0 R >>")

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for indice, objeto in enumerate(objetos, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{indice} 0 obj\n".encode("ascii"))
        pdf.extend(objeto)
        pdf.extend(b"\nendobj\n")

    inicio_xref = len(pdf)
    pdf.extend(f"xref\n0 {len(objetos) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))

    titulo_limpo = normalizar_pdf_texto(titulo) or "documento"
    pdf.extend(
        (
            "trailer\n<< /Size {size} /Root {root} 0 R /Info << /Title ({title}) >> >>\n"
            "startxref\n{xref}\n%%EOF"
        ).format(
            size=len(objetos) + 1,
            root=catalogo_id,
            title=escapar_pdf_texto(titulo_limpo),
            xref=inicio_xref
        ).encode("latin-1")
    )
    return bytes(pdf)


def montar_linhas_pdf_ordem(ordem, itens):
    total_os = sum(float(item["subtotal"] or 0) for item in itens)
    endereco = " ".join(
        part for part in [
            ordem["endereco"],
            ordem["numero"],
            ordem["bairro"],
            ordem["cidade"],
        ] if str(part or "").strip()
    )

    linhas = [
        f"ORDEM DE SERVICO #{ordem['id']}",
        "",
        f"Status: {normalizar_pdf_texto(ordem['status'] or 'Sem status')}",
        f"Data: {normalizar_pdf_texto(ordem['data_agendamento'] or '-')}    Hora: {normalizar_pdf_texto(ordem['hora'] or '-')}",
        f"Cliente: {normalizar_pdf_texto(ordem['nome'] or 'Nao informado')}",
        f"Telefone: {normalizar_pdf_texto(ordem['telefone'] or 'Nao informado')}",
        f"Endereco: {normalizar_pdf_texto(endereco or 'Nao informado')}",
        f"Produto/Servico: {normalizar_pdf_texto(ordem['servico_nome'] or 'Nao informado')}",
        f"Defeito: {normalizar_pdf_texto(ordem['defeito_reclamado'] or 'Nao informado')}",
    ]

    if ordem["observacao"]:
        linhas.append("Observacoes:")
        linhas.extend(textwrap.wrap(normalizar_pdf_texto(ordem["observacao"]), width=95) or [""])

    linhas.extend([
        "",
        "ITENS",
        "-" * 95,
        f"{'Descricao':<46} {'Qtd':>5} {'Valor':>14} {'Subtotal':>14}",
        "-" * 95,
    ])

    if itens:
        for item in itens:
            descricao = normalizar_pdf_texto(item["descricao"] or "Item")
            quantidade = str(item["quantidade"] or 0)
            valor = f"R$ {float(item['valor'] or 0):.2f}"
            subtotal = f"R$ {float(item['subtotal'] or 0):.2f}"
            blocos = textwrap.wrap(descricao, width=46) or [""]
            for idx, trecho in enumerate(blocos):
                if idx == 0:
                    linhas.append(f"{trecho:<46} {quantidade:>5} {valor:>14} {subtotal:>14}")
                else:
                    linhas.append(trecho)
    else:
        linhas.append("Nenhum item adicionado a esta OS.")

    linhas.extend([
        "-" * 95,
        f"{'TOTAL DA ORDEM:':>79} R$ {total_os:.2f}",
        "",
        "ASSINATURA DO CLIENTE",
        normalizar_pdf_texto(ordem["assinatura_nome"] or "________________________________________"),
    ])

    if ordem["assinatura_data"]:
        linhas.append(f"Data da assinatura: {normalizar_pdf_texto(ordem['assinatura_data'])}")

    return linhas


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
        bootstrap_password = (os.environ.get("BOOTSTRAP_OWNER_PASSWORD") or "").strip() or secrets.token_urlsafe(18)
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
            generate_password_hash(bootstrap_password),
            "proprietario"
        ))
        print(
            "Bootstrap de proprietario criado com senha temporaria segura. "
            "CPF: 00000000000. Defina BOOTSTRAP_OWNER_PASSWORD para controlar esse valor. "
            f"Senha temporaria: {bootstrap_password}"
        )

    conn.commit()
    conn.close()
    AUTH_SCHEMA_READY = True


def ensure_ordem_servico_core_columns():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(ordem_servico)")
    cols = {c["name"] for c in cursor.fetchall()}

    if "cliente_id" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN cliente_id INTEGER")
    if "hora" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN hora TEXT")
    if "tecnico_id" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN tecnico_id INTEGER")
    if "defeito_reclamado" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN defeito_reclamado TEXT")
    if "produto_defeito" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN produto_defeito TEXT")
    if "observacao" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN observacao TEXT")
    if "diagnostico" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN diagnostico TEXT")
    if "solucao" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN solucao TEXT")
    if "pecas_trocadas" not in cols:
        cursor.execute("ALTER TABLE ordem_servico ADD COLUMN pecas_trocadas TEXT")

    conn.commit()
    conn.close()


def ensure_ordem_servico_mobile_columns():
    global OS_MOBILE_SCHEMA_READY
    if OS_MOBILE_SCHEMA_READY:
        return

    ensure_ordem_servico_core_columns()
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
    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and not is_same_origin_request():
        return resposta_acesso_negado("Requisicao bloqueada por validacao de origem.")
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
        rate_key, retry_after = enforce_auth_rate_limit("login_funcionario", cpf or client_ip())
        if retry_after:
            erro = f"Muitas tentativas. Tente novamente em {retry_after}s."
            return render_template("login.html", erro=erro), 429

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
            register_auth_failure(rate_key)
            erro = "Usuario nao encontrado."
        elif (user["status"] or "Ativo") != "Ativo":
            register_auth_failure(rate_key)
            erro = "Usuario inativo."
        elif not user["senha_hash"] or not check_password_hash(user["senha_hash"], senha):
            register_auth_failure(rate_key)
            erro = "Senha invalida."
        else:
            clear_auth_failures(rate_key)
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


@app.route("/assistencia_tecnica")
def assistencia_tecnica():
    clear_assistencia_state()
    ensure_clientes_portal_columns()
    cliente = get_portal_cliente()
    customer_data = hydrate_assistencia_from_portal(get_assistencia_state())
    chamados = listar_chamados_cliente_portal(cliente["id"]) if cliente else []
    return render_template(
        "assistencia_tecnica.html",
        portal_cliente=dict(cliente) if cliente else None,
        portal_chamados=chamados,
        initial_chat_message=mensagem_inicial_assistencia(customer_data),
    )


@app.route("/assistencia_tecnica/cliente/login", methods=["POST"])
def assistencia_cliente_login():
    ensure_clientes_portal_columns()
    dados = request.get_json(silent=True) or request.form
    login = str(dados.get("login") or "").strip()
    senha = str(dados.get("senha") or "").strip()
    rate_key, retry_after = enforce_auth_rate_limit("login_portal", login or client_ip())
    if retry_after:
        return jsonify({"ok": False, "erro": f"Muitas tentativas. Tente novamente em {retry_after}s."}), 429
    if not login or not senha:
        register_auth_failure(rate_key)
        return jsonify({"ok": False, "erro": "Informe email ou telefone e a senha."}), 400

    cliente = buscar_cliente_portal_por_login(login)
    if not cliente or not str(cliente["senha_hash"] or "").strip():
        register_auth_failure(rate_key)
        return jsonify({"ok": False, "erro": "Cadastro nao encontrado ou sem senha configurada."}), 404
    if not check_password_hash(cliente["senha_hash"], senha):
        register_auth_failure(rate_key)
        return jsonify({"ok": False, "erro": "Senha incorreta."}), 400

    clear_auth_failures(rate_key)
    set_portal_cliente_session(cliente["id"])
    return jsonify({"ok": True})


@app.route("/assistencia_tecnica/cliente/cadastro", methods=["POST"])
def assistencia_cliente_cadastro():
    ensure_clientes_portal_columns()
    dados = request.get_json(silent=True) or request.form
    nome = str(dados.get("nome") or "").strip()
    telefone = normalize_phone_digits(dados.get("telefone"))
    email = str(dados.get("email") or "").strip().lower()
    senha = str(dados.get("senha") or "").strip()

    if not nome or len(telefone) < 12 or not email or len(senha) < 4:
        return jsonify({"ok": False, "erro": "Preencha nome, telefone com DDD, email e uma senha com pelo menos 4 caracteres."}), 400

    existente_telefone = buscar_cliente_portal_por_login(telefone)
    existente_email = buscar_cliente_portal_por_login(email)
    cliente = existente_telefone or existente_email

    conn = get_db()
    cursor = conn.cursor()
    if cliente:
        if str(cliente["senha_hash"] or "").strip():
            conn.close()
            return jsonify({"ok": False, "erro": "Ja existe uma conta com esse email ou telefone."}), 400
        cursor.execute("""
            UPDATE clientes
            SET nome = ?, telefone = ?, whatsapp = ?, email = ?, senha_hash = ?
            WHERE id = ?
        """, (nome, telefone[-11:], telefone[-11:], email, generate_password_hash(senha), cliente["id"]))
        cliente_id = cliente["id"]
    else:
        cursor.execute("""
            INSERT INTO clientes (
                tipo, nome, telefone, whatsapp, email, observacoes, data_cadastro, senha_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "PF",
            nome,
            telefone[-11:],
            telefone[-11:],
            email,
            "Cadastro criado pelo portal do cliente",
            datetime.now().strftime("%Y-%m-%d"),
            generate_password_hash(senha),
        ))
        cliente_id = cursor.lastrowid
    conn.commit()
    conn.close()

    set_portal_cliente_session(cliente_id)
    return jsonify({"ok": True})


@app.route("/assistencia_tecnica/cliente/recuperar_senha", methods=["POST"])
def assistencia_cliente_recuperar_senha():
    return jsonify({
        "ok": False,
        "erro": "Recuperacao de senha online desativada por seguranca. Solicite a redefinicao pelo atendimento da loja."
    }), 403


@app.route("/assistencia_tecnica/cliente/logout", methods=["POST"])
def assistencia_cliente_logout():
    clear_portal_cliente_session()
    return jsonify({"ok": True})


@app.route("/assistencia_tecnica/cliente/chamado", methods=["POST"])
def assistencia_cliente_chamado():
    ensure_clientes_portal_columns()
    cliente = get_portal_cliente()
    if not cliente:
        return jsonify({"ok": False, "erro": "Faca login para abrir um chamado."}), 401

    dados = request.get_json(silent=True) or request.form
    try:
        chamado_id, tecnico_nome = criar_chamado_portal_cliente(cliente, dados)
    except ValueError as exc:
        return jsonify({"ok": False, "erro": str(exc)}), 400

    return jsonify({
        "ok": True,
        "mensagem": f"Chamado #{chamado_id} aberto com sucesso. Tecnico responsavel inicial: {tecnico_nome}.",
    })


@app.route("/assistencia_tecnica/chat", methods=["POST"])
def assistencia_tecnica_chat():
    dados = request.get_json(silent=True) or {}
    prompt = (dados.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"answer": "Envie uma mensagem primeiro."}), 400

    customer_data = hydrate_assistencia_from_portal(get_assistencia_state())
    intent = detect_assistencia_intent(prompt)
    if intent and not customer_data.get("intent"):
        customer_data["intent"] = intent
        save_assistencia_state(customer_data)
    service_type = detect_service_type(prompt)
    if service_type and not customer_data.get("service_type"):
        customer_data["service_type"] = service_type
        save_assistencia_state(customer_data)

    if intent in ASSISTENCIA_FAQ:
        extracted = extract_assistencia_fields(prompt)
        for field in ("nome", "whatsapp", "tipo_equipamento", "marca", "modelo"):
            if field in extracted and not str(customer_data.get(field) or "").strip():
                customer_data[field] = extracted[field]
        cidade_interesse = extract_city_from_message(prompt)
        if cidade_interesse:
            customer_data["cidade_interesse"] = cidade_interesse
        resposta = responder_faq_assistencia(intent, prompt, customer_data)
        if intent == "produto":
            clear_assistencia_state()
            customer_data = get_assistencia_state()
            customer_data["pending_action"] = "aguardando_produto_consulta"
            save_assistencia_state(customer_data)
            if "Nao encontrei esse produto" in resposta:
                return jsonify({"answer": resposta})
            return jsonify({"answer": f"{resposta}\n\nSe quiser, pode mandar outro codigo, sku ou nome de produto."})
        if intent == "entrega":
            clear_assistencia_state()
            return jsonify({"answer": resposta})
        if not should_autostart_booking(customer_data, prompt, extracted):
            save_assistencia_state(customer_data)
            customer_data["pending_action"] = "oferecer_agendamento"
            save_assistencia_state(customer_data)
            return jsonify({"answer": f"{resposta}\n\nSe quiser, eu posso fazer seu agendamento agora. Responda 'sim' para continuar ou mande outra pergunta."})
        proxima = proxima_pergunta_assistencia(customer_data)
        if proxima:
            save_assistencia_state(customer_data)
            return jsonify({"answer": f"{resposta}\n\n{proxima}"})
        clear_assistencia_state()
        return jsonify({"answer": f"{resposta}\n\nSe quiser, eu tambem posso continuar e fazer seu agendamento agora."})

    if should_treat_as_product_query(customer_data, prompt):
        clear_assistencia_state()
        customer_data = get_assistencia_state()
        customer_data["pending_action"] = "aguardando_produto_consulta"
        save_assistencia_state(customer_data)
        resposta = responder_faq_assistencia("produto", prompt, customer_data)
        return jsonify({"answer": f"{resposta}\n\nSe quiser, pode mandar outro codigo, sku ou nome de produto."})

    current_field = next_assistencia_field(customer_data)
    pending_action = customer_data.get("pending_action")
    resposta_prefixo = ""

    if current_field == "nome" and is_assistencia_greeting(prompt):
        return jsonify({"answer": f"{get_assistencia_greeting_message()}\n{ASSISTENCIA_QUESTIONS['nome']}"})

    if intent == "cancelar_fluxo":
        clear_assistencia_state()
        return jsonify({"answer": "Tudo bem. Pode fazer sua pergunta. Se quiser retomar o agendamento depois, eu continuo por aqui."})

    if pending_action == "aguardando_produto_consulta":
        if intent == "cancelar_fluxo":
            clear_assistencia_state()
            return jsonify({"answer": "Tudo bem. Se quiser retomar depois, pode mandar outra pergunta."})
        if intent and intent != "produto":
            clear_assistencia_state()
            customer_data = get_assistencia_state()
            if intent:
                customer_data["intent"] = intent
                save_assistencia_state(customer_data)
        else:
            resposta = responder_faq_assistencia("produto", prompt, customer_data)
            if "Nao encontrei esse produto" in resposta:
                save_assistencia_state(customer_data)
                return jsonify({"answer": resposta})
            save_assistencia_state(customer_data)
            return jsonify({"answer": f"{resposta}\n\nSe quiser, pode mandar outro codigo, sku ou nome de produto."})

    if pending_action == "oferecer_agendamento":
        if is_assistencia_yes(prompt):
            customer_data["intent"] = "agendar"
            customer_data.pop("pending_action", None)
            save_assistencia_state(customer_data)
            proxima = proxima_pergunta_assistencia(customer_data)
            if proxima:
                return jsonify({"answer": proxima})
            return jsonify({"answer": ASSISTENCIA_QUESTIONS["nome"]})
        if detect_assistencia_intent(prompt) in ASSISTENCIA_FAQ:
            customer_data.pop("pending_action", None)
            save_assistencia_state(customer_data)
        else:
            return jsonify({"answer": "Tudo bem. Se quiser agendar depois, responda 'sim'. Se preferir, pode mandar outra pergunta."})

    if pending_action == "confirmar_endereco_cadastrado":
        if is_assistencia_yes(prompt):
            customer_data["usar_endereco_cadastrado"] = "1"
            customer_data.pop("pending_action", None)
            save_assistencia_state(customer_data)
            return jsonify({"answer": ASSISTENCIA_QUESTIONS["tipo_equipamento"]})
        if is_assistencia_no(prompt):
            clear_assistencia_from_field(customer_data, "endereco")
            customer_data["usar_endereco_cadastrado"] = "0"
            customer_data.pop("pending_action", None)
            save_assistencia_state(customer_data)
            return jsonify({"answer": ASSISTENCIA_QUESTIONS["tipo_equipamento"]})
        return jsonify({"answer": "Responda 'sim' para usar o endereco cadastrado ou 'nao' para informar outro."})

    if pending_action == "aguardando_whatsapp_os":
        whatsapp = normalize_phone_digits(prompt)
        if len(whatsapp) < 12:
            return jsonify({"answer": "Informe o WhatsApp com DDD para localizar sua OS."})
        customer_data["whatsapp"] = whatsapp[-11:]
        os_row = buscar_os_aberta_por_whatsapp(whatsapp)
        if not os_row:
            clear_assistencia_state()
            return jsonify({"answer": "Nao encontrei OS aberta para esse WhatsApp. Se quiser, podemos iniciar um novo agendamento."})
        customer_data["os_existente_id"] = os_row["id"]
        customer_data["cliente_id"] = os_row["cliente_id"]
        if customer_data.get("intent") == "consultar_os":
            resumo = resumo_os_existente(os_row)
            clear_assistencia_state()
            return jsonify({"answer": resumo})
        customer_data["pending_action"] = "reagendar_dia"
        save_assistencia_state(customer_data)
        return jsonify({"answer": f"{resumo_os_existente(os_row)}\nQual nova data voce deseja?"})

    if pending_action == "reagendar_dia":
        parsed_schedule = parse_assistencia_schedule(prompt)
        customer_data["dia"] = parsed_schedule["day"]
        customer_data["preferred_slots"] = parsed_schedule["preferred_slots"]
        customer_data["pending_action"] = "reagendar_horario"
        save_assistencia_state(customer_data)
        horarios_lista = filtrar_horarios_preferidos(
            horarios_disponiveis_assistencia(customer_data.get("dia")),
            customer_data.get("preferred_slots"),
        )
        if not horarios_lista:
            customer_data["pending_action"] = "reagendar_dia"
            customer_data["dia"] = ""
            save_assistencia_state(customer_data)
            return jsonify({"answer": "Nao ha horarios livres nessa data. Informe outro dia para remarcar."})
        return jsonify({"answer": build_assistencia_horario_prompt(customer_data, horarios_lista)})

    if pending_action == "reagendar_horario":
        horarios_lista = filtrar_horarios_preferidos(
            horarios_disponiveis_assistencia(customer_data.get("dia")),
            customer_data.get("preferred_slots"),
        )
        horario_escolhido = parse_assistencia_slot_choice(prompt, horarios_lista)
        if not horario_escolhido:
            return jsonify({"answer": f"Escolha um horario valido.\nHorarios disponiveis:\n{chr(10).join(horarios_lista)}"})
        customer_data["horario"] = horario_escolhido
        customer_data["pending_action"] = "confirmar_reagendamento"
        save_assistencia_state(customer_data)
        return jsonify({"answer": f"Posso remarcar sua OS para {format_assistencia_day(customer_data.get('dia'))} as {customer_data.get('horario')}? Responda 'sim' ou 'nao'."})

    if pending_action == "confirmar_reagendamento":
        if is_assistencia_yes(prompt):
            tecnico = tecnico_disponivel_assistencia(customer_data)
            if tecnico is None and listar_tecnicos_assistencia():
                customer_data["horario"] = ""
                customer_data["pending_action"] = "reagendar_horario"
                save_assistencia_state(customer_data)
                horarios = filtrar_horarios_preferidos(
                    horarios_disponiveis_assistencia(customer_data.get("dia")),
                    customer_data.get("preferred_slots"),
                )
                return jsonify({"answer": f"Esse horario nao esta mais disponivel.\nHorarios livres:\n{chr(10).join(horarios)}"})
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE ordem_servico
                SET data_agendamento = ?, hora = ?, tecnico_id = ?
                WHERE id = ?
            """, (
                customer_data.get("dia"),
                customer_data.get("horario"),
                tecnico["id"] if tecnico else None,
                customer_data.get("os_existente_id"),
            ))
            conn.commit()
            conn.close()
            resposta = (
                f"OS #{customer_data.get('os_existente_id')} reagendada com sucesso.\n"
                f"Nova data: {format_assistencia_day(customer_data.get('dia'))} as {customer_data.get('horario')}."
            )
            clear_assistencia_state()
            return jsonify({"answer": resposta})
        if is_assistencia_no(prompt):
            customer_data["pending_action"] = "reagendar_dia"
            customer_data["dia"] = ""
            customer_data["horario"] = ""
            save_assistencia_state(customer_data)
            return jsonify({"answer": "Tudo bem. Qual nova data voce deseja?"})
        return jsonify({"answer": "Responda 'sim' para confirmar o reagendamento ou 'nao' para alterar."})

    if pending_action == "aguardando_numero":
        numero = prompt.strip()
        if not numero or is_probable_cep(numero):
            return jsonify({"answer": "Informe o numero do endereco."})
        customer_data["numero"] = numero
        customer_data["endereco"] = montar_endereco_assistencia(customer_data)
        customer_data["pending_action"] = "aguardando_complemento"
        save_assistencia_state(customer_data)
        return jsonify({"answer": "Se tiver complemento, informe agora. Se nao tiver, responda 'nao'."})

    if pending_action == "aguardando_complemento":
        if is_assistencia_no(prompt):
            customer_data["complemento"] = ""
        else:
            customer_data["complemento"] = prompt.strip()
        customer_data["endereco"] = montar_endereco_assistencia(customer_data)
        customer_data.pop("pending_action", None)
        save_assistencia_state(customer_data)
        return jsonify({"answer": get_assistencia_question("dia")})

    if pending_action == "triagem_tecnica":
        perguntas = customer_data.get("triagem_perguntas") or []
        indice = int(customer_data.get("triagem_indice") or 0)
        if indice < len(perguntas):
            respostas = customer_data.get("triagem_respostas") or []
            respostas.append({
                "pergunta": perguntas[indice],
                "resposta": prompt.strip(),
            })
            customer_data["triagem_respostas"] = respostas
            indice += 1
            customer_data["triagem_indice"] = indice

        if indice < len(perguntas):
            save_assistencia_state(customer_data)
            return jsonify({"answer": perguntas[indice]})

        customer_data.pop("pending_action", None)
        save_assistencia_state(customer_data)
        return jsonify({"answer": f"{customer_data['nome']}, qual e o endereco para o atendimento? Voce pode informar o endereco completo ou enviar o CEP."})

    if pending_action == "confirmar_agendamento":
        if is_assistencia_yes(prompt):
            tecnico = tecnico_disponivel_assistencia(customer_data)
            if tecnico is None and listar_tecnicos_assistencia():
                customer_data["horario"] = ""
                customer_data.pop("pending_action", None)
                save_assistencia_state(customer_data)
                horarios = descricao_disponibilidade_horarios(customer_data.get("dia"))
                if horarios == "Nenhum horario disponivel":
                    customer_data["dia"] = ""
                    save_assistencia_state(customer_data)
                    return jsonify({"answer": "Nao ha horarios livres nessa data. Escolha outro dia para o atendimento."})
                return jsonify({"answer": build_assistencia_horario_prompt(customer_data, horarios, "Esse horario acabou de ficar indisponivel.\n")})

            if tecnico:
                customer_data["tecnico_sugerido_id"] = tecnico["id"]
                customer_data["tecnico_sugerido_nome"] = tecnico["nome"]
            os_id, tecnico = criar_os_assistencia(customer_data)
            whatsapp_loja = get_loja_whatsapp()
            tecnico_nome = tecnico["nome"] if tecnico else "da equipe"
            endereco_atendimento = montar_endereco_assistencia(customer_data) or customer_data.get("endereco", "")
            summary = (
                "Agendamento confirmado.\n"
                f"OS #{os_id} criada com sucesso.\n"
                f"Tecnico responsavel: {tecnico_nome}.\n"
                f"Data: {format_assistencia_day(customer_data['dia'])} as {customer_data['horario']}.\n"
                f"Equipamento: {customer_data['tipo_equipamento']}.\n"
                f"Marca: {customer_data['marca']}.\n"
                f"Modelo: {customer_data['modelo']}.\n"
                f"Problema: {customer_data['problema']}.\n"
                f"Endereco: {endereco_atendimento}.\n"
                f"Confirmacao pelo WhatsApp da loja: {whatsapp_loja}."
            )
            clear_assistencia_state()
            return jsonify({"answer": summary, "os_id": os_id})
        if is_assistencia_no(prompt):
            customer_data["pending_action"] = "editar_campo"
            save_assistencia_state(customer_data)
            return jsonify({"answer": "Qual dado voce quer alterar? Exemplo: nome, whatsapp, equipamento, marca, modelo, problema, endereco, dia ou horario."})
        return jsonify({"answer": "Responda 'sim' para confirmar ou 'nao' para alterar algum dado."})

    if pending_action == "editar_campo":
        field = map_assistencia_edit_field(prompt)
        if not field:
            return jsonify({"answer": "Informe qual dado deseja alterar: nome, whatsapp, equipamento, marca, modelo, problema, endereco, dia ou horario."})
        clear_assistencia_from_field(customer_data, field)
        customer_data.pop("pending_action", None)
        save_assistencia_state(customer_data)
        if field == "endereco":
            return jsonify({"answer": f"{customer_data.get('nome', '').strip()}, qual e o endereco para o atendimento? Voce pode informar o endereco completo ou enviar o CEP."})
        return jsonify({"answer": get_assistencia_question(field)})

    if customer_data.get("intent") in {"reagendar", "consultar_os"}:
        whatsapp_extraido = extract_assistencia_fields(prompt).get("whatsapp")
        if whatsapp_extraido and not customer_data.get("whatsapp"):
            customer_data["whatsapp"] = whatsapp_extraido
            save_assistencia_state(customer_data)
        if customer_data.get("cliente_id"):
            os_row = buscar_os_aberta_por_cliente(customer_data.get("cliente_id"))
            if os_row:
                customer_data["os_existente_id"] = os_row["id"]
                customer_data["cliente_id"] = os_row["cliente_id"]
                if os_row["whatsapp"] and not customer_data.get("whatsapp"):
                    customer_data["whatsapp"] = str(os_row["whatsapp"]).strip()
                if customer_data.get("intent") == "consultar_os":
                    resumo = resumo_os_existente(os_row)
                    clear_assistencia_state()
                    return jsonify({"answer": resumo})
                customer_data["pending_action"] = "reagendar_dia"
                save_assistencia_state(customer_data)
                return jsonify({"answer": f"{resumo_os_existente(os_row)}\nQual nova data voce deseja?"})
        if customer_data.get("whatsapp"):
            customer_data["pending_action"] = "aguardando_whatsapp_os"
            save_assistencia_state(customer_data)
            return assistencia_tecnica_chat()
        customer_data["pending_action"] = "aguardando_whatsapp_os"
        save_assistencia_state(customer_data)
        if customer_data.get("intent") == "consultar_os":
            return jsonify({"answer": "Para consultar sua OS, informe o WhatsApp com DDD cadastrado."})
        return jsonify({"answer": "Para reagendar sua OS, informe o WhatsApp com DDD cadastrado."})

    if current_field:
        extracted = extract_assistencia_fields(prompt)
        if current_field == "nome":
            if extracted.get("whatsapp") and not extracted.get("nome") and len(normalize_phone_digits(prompt)) >= 12:
                customer_data["whatsapp"] = extracted["whatsapp"]
                save_assistencia_state(customer_data)
                return jsonify({"answer": "Antes de continuar, me diga seu nome."})
            for field in ASSISTENCIA_FLOW:
                if field in extracted and not str(customer_data.get(field) or "").strip():
                    customer_data[field] = extracted[field]
            current_field = next_assistencia_field(customer_data)
            triagem_inicial = iniciar_triagem_se_necessario(customer_data)
            if triagem_inicial:
                save_assistencia_state(customer_data)
                return jsonify({"answer": triagem_inicial.strip()})
            if current_field != "nome" and extracted:
                save_assistencia_state(customer_data)

        if current_field == "whatsapp":
            whatsapp = normalize_phone_digits(prompt)
            if len(whatsapp) < 12:
                return jsonify({"answer": "Informe um WhatsApp valido com DDD. Exemplo: 11999998888."})
            customer_data[current_field] = whatsapp[-11:]
            cliente = buscar_cliente_assistencia_por_whatsapp(whatsapp)
            if cliente:
                customer_data["cliente_id"] = cliente["id"]
                customer_data["cep"] = str(cliente["cep"] or "").strip()
                nome_cadastrado = (cliente["nome"] or cliente["razao_social"] or "").strip()
                if nome_cadastrado:
                    customer_data["nome"] = nome_cadastrado

                endereco_cadastrado = formatar_endereco_cliente(cliente)
                if endereco_cadastrado:
                    customer_data["endereco"] = endereco_cadastrado
                    customer_data["logradouro"] = str(cliente["endereco"] or "").strip()
                    customer_data["numero"] = str(cliente["numero"] or "").strip()
                    customer_data["complemento"] = str(cliente["complemento"] or "").strip()
                    customer_data["bairro"] = str(cliente["bairro"] or "").strip()
                    customer_data["cidade"] = str(cliente["cidade"] or "").strip()
                    customer_data["pending_action"] = "confirmar_endereco_cadastrado"
                    save_assistencia_state(customer_data)
                    return jsonify({
                        "answer": (
                            f"Encontrei seu cadastro com este endereco: {endereco_cadastrado}.\n"
                            "Deseja usar esse endereco? Responda 'sim' ou 'nao'."
                        )
                    })
        elif current_field == "endereco":
            cep_info = buscar_endereco_por_cep(prompt)
            if cep_info:
                endereco_formatado = formatar_endereco_cep(cep_info)
                customer_data["cep"] = cep_info["cep"]
                customer_data["logradouro"] = cep_info["logradouro"]
                customer_data["bairro"] = cep_info["bairro"]
                customer_data["cidade"] = cep_info["cidade"]
                customer_data["endereco_base"] = endereco_formatado
                customer_data["pending_action"] = "aguardando_numero"
                save_assistencia_state(customer_data)
                return jsonify({
                    "answer": (
                        f"Encontrei este endereco pelo CEP: {endereco_formatado}.\n"
                        "Agora me informe o numero do endereco."
                    )
                })

            if is_probable_cep(prompt):
                save_assistencia_state(customer_data)
                return jsonify({
                    "answer": (
                        "Nao consegui localizar esse CEP agora.\n"
                        "Envie outro CEP ou informe o endereco completo com rua e numero."
                    )
                })

            endereco_manual = parse_endereco_manual(prompt)
            customer_data["logradouro"] = endereco_manual["logradouro"]
            customer_data["numero"] = endereco_manual["numero"]
            customer_data["complemento"] = endereco_manual["complemento"]
            customer_data[current_field] = montar_endereco_assistencia(customer_data) or prompt
        elif current_field == "horario":
            horarios_lista = filtrar_horarios_preferidos(
                horarios_disponiveis_assistencia(customer_data.get("dia")),
                customer_data.get("preferred_slots"),
            )
            horario_escolhido = parse_assistencia_slot_choice(prompt, horarios_lista)
            if not horario_escolhido:
                return jsonify({"answer": f"Escolha um horario valido.\nHorarios disponiveis:\n{chr(10).join(horarios_lista or ASSISTENCIA_SLOTS)}"})
            customer_data[current_field] = horario_escolhido
        elif current_field == "dia":
            parsed_schedule = parse_assistencia_schedule(prompt)
            customer_data[current_field] = parsed_schedule["day"]
            customer_data["preferred_slots"] = parsed_schedule["preferred_slots"]
        elif current_field == "problema":
            customer_data[current_field] = prompt
            triagem_inicial = iniciar_triagem_se_necessario(customer_data)
            if triagem_inicial:
                save_assistencia_state(customer_data)
                return jsonify({"answer": triagem_inicial.strip()})
            if customer_data.get("alerta_urgencia"):
                resposta_prefixo = f"{customer_data['alerta_urgencia']}\n\n"
        else:
            customer_data[current_field] = prompt
        save_assistencia_state(customer_data)

    upcoming_field = next_assistencia_field(customer_data)
    if upcoming_field:
        if upcoming_field == "endereco":
            return jsonify({"answer": f"{resposta_prefixo}{customer_data['nome']}, qual e o endereco para o atendimento? Voce pode informar o endereco completo ou enviar o CEP."})
        if upcoming_field == "horario":
            horarios_lista = filtrar_horarios_preferidos(
                horarios_disponiveis_assistencia(customer_data.get("dia")),
                customer_data.get("preferred_slots"),
            )
            if not horarios_lista:
                customer_data["dia"] = ""
                save_assistencia_state(customer_data)
                return jsonify({"answer": f"{resposta_prefixo}Nao ha horarios livres nessa data. Escolha outro dia para o atendimento."})
            return jsonify({"answer": build_assistencia_horario_prompt(customer_data, horarios_lista, resposta_prefixo)})
        return jsonify({"answer": f"{resposta_prefixo}{get_assistencia_question(upcoming_field)}"})

    tecnico = tecnico_disponivel_assistencia(customer_data)
    if tecnico is None and listar_tecnicos_assistencia():
        horarios_lista = filtrar_horarios_preferidos(
            horarios_disponiveis_assistencia(customer_data.get("dia")),
            customer_data.get("preferred_slots"),
        )
        customer_data["horario"] = ""
        save_assistencia_state(customer_data)
        if not horarios_lista:
            customer_data["dia"] = ""
            save_assistencia_state(customer_data)
            return jsonify({"answer": "Nao ha horarios livres nessa data. Escolha outro dia para o atendimento."})
        return jsonify({"answer": build_assistencia_horario_prompt(customer_data, horarios_lista, "Esse horario nao esta mais disponivel.\n")})

    if tecnico:
        customer_data["tecnico_sugerido_id"] = tecnico["id"]
        customer_data["tecnico_sugerido_nome"] = tecnico["nome"]
    customer_data["pending_action"] = "confirmar_agendamento"
    save_assistencia_state(customer_data)
    return jsonify({"answer": resumo_assistencia(customer_data)})


@app.route("/assistencia_tecnica/reset", methods=["POST"])
def assistencia_tecnica_reset():
    clear_assistencia_state()
    customer_data = hydrate_assistencia_from_portal(get_assistencia_state())
    return jsonify({"ok": True, "answer": mensagem_inicial_assistencia(customer_data)})


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
@app.route("/excluir_fornecedor/<int:id>", methods=["POST"])
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
    ensure_ordem_servico_mobile_columns()
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

    itens = carregar_itens_pedido(cursor, id)
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
    ensure_ordem_servico_mobile_columns()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT m.*, COALESCE(f.nome, '') AS funcionario_nome
        FROM movimentos m
        LEFT JOIN funcionarios f ON m.funcionario_id = f.id
        WHERE m.id=?
    """, (id,))
    pedido = cursor.fetchone()

    itens = carregar_itens_pedido(cursor, id)

    cursor.execute("SELECT * FROM loja_config WHERE id=1")
    loja = cursor.fetchone()

    data = datetime.now().strftime("%d/%m/%Y %H:%M")
    total_pedido = float(pedido["total"] or 0) if pedido else 0.0
    if total_pedido <= 0 and itens:
        total_pedido = sum(float(item["subtotal"] or 0) for item in itens)
    conn.close()

    return render_template("cupom.html",
                           pedido=pedido,
                           itens=itens,
                           data=data,
                           loja=loja,
                           total_pedido=total_pedido)
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
    cep = (dados.get("cep") or "").strip()
    endereco = (dados.get("endereco") or "").strip()
    bairro = (dados.get("bairro") or "").strip()
    cidade = (dados.get("cidade") or "").strip()
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
        cep,
        endereco,
        numero,
        bairro,
        cidade,
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
        "cpf": cpf,
        "cep": cep,
        "endereco": endereco,
        "bairro": bairro,
        "cidade": cidade,
        "numero": numero
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
@app.route("/atualizar_status/<int:id>/<status>", methods=["POST"])
def atualizar_status(id, status):
    if status not in ASSISTENCIA_ALLOWED_OS_STATUS:
        return resposta_acesso_negado("Status invalido.")

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
@app.route("/mudar_status/<int:id>/<status>", methods=["POST"])
def mudar_status(id, status):
    if status not in ASSISTENCIA_ALLOWED_OS_STATUS:
        return resposta_acesso_negado("Status invalido.")

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
           c.whatsapp,
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


@app.route("/ordem/<int:id>/pdf")
def ordem_pdf(id):

    conn = get_db()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
    SELECT os.*,
           c.nome,
           c.telefone,
           c.whatsapp,
           c.endereco,
           c.numero,
           c.bairro,
           c.cidade
    FROM ordem_servico os
    LEFT JOIN clientes c ON os.cliente_id = c.id
    WHERE os.id = ?
    """, (id,))

    ordem = cursor.fetchone()
    if not ordem:
        conn.close()
        return "OS nao encontrada", 404

    cursor.execute("""
    SELECT *
    FROM itens_ordem_servico
    WHERE os_id = ?
    ORDER BY id
    """, (id,))

    itens = cursor.fetchall()
    conn.close()

    linhas = montar_linhas_pdf_ordem(ordem, itens)
    pdf_bytes = gerar_pdf_texto_simples(linhas, titulo=f"OS {id}")
    nome_arquivo = f"os_{id}.pdf"

    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{nome_arquivo}"'
        }
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
    debug = (os.environ.get("FLASK_DEBUG", "0").strip().lower() in ("1", "true", "yes", "on"))

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
