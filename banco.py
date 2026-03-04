import sqlite3

def conectar():
    conn = sqlite3.connect("database.db")
    conn.row_factory = sqlite3.Row   # ← ADICIONE ISSO
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def criar_tabelas():
    conn = conectar()
    cursor = conn.cursor()

    # Tabela Categorias
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categorias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        categoria_pai_id INTEGER,
        FOREIGN KEY (categoria_pai_id) REFERENCES categorias(id)
    )
    """)

    # Tabela Fornecedores
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fornecedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        razao_social TEXT NOT NULL,
        nome_fantasia TEXT,
        cnpj TEXT,
        telefone TEXT,
        email TEXT
    )
    """)

    # Tabela Produtos
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS produtos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT UNIQUE NOT NULL,
        nome TEXT NOT NULL,
        codigo_barras TEXT,
        categoria_id INTEGER,

        preco_custo REAL NOT NULL,
        preco_venda REAL NOT NULL,
        margem_lucro REAL,

        peso REAL,
        altura REAL,
        largura REAL,
        profundidade REAL,

        estoque_minimo INTEGER,
        estoque_maximo INTEGER,
        localizacao TEXT,
        unidade TEXT,

        ncm TEXT,
        cest TEXT,
        origem TEXT,
        icms REAL,
        ipi REAL,

        descricao TEXT,
        imagem TEXT,
        ativo INTEGER DEFAULT 1,

        fornecedor_id INTEGER,

        FOREIGN KEY (categoria_id) REFERENCES categorias(id),
        FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id)
    )
    """)

    # Tabela Estoque Movimentações
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS estoque_movimentacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        produto_id INTEGER,
        tipo TEXT,
        quantidade INTEGER,
        data DATETIME DEFAULT CURRENT_TIMESTAMP,
        observacao TEXT,
        FOREIGN KEY (produto_id) REFERENCES produtos(id)
    )
    """)

    # Tabela Produto Variações
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS produto_variacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        produto_id INTEGER,
        sku_variacao TEXT UNIQUE,
        nome_variacao TEXT,
        valor_variacao TEXT,
        FOREIGN KEY (produto_id) REFERENCES produtos(id)
    )
    """)
    # Criar tabela ordem_servico
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ordem_servico (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         servico_nome TEXT NOT NULL,
         data_agendamento TEXT NOT NULL,
         status TEXT DEFAULT 'Agendado',
         data_criacao TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()