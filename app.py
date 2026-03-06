from flask import Flask, render_template, request, redirect, jsonify
from banco import criar_tabelas, conectar
from datetime import datetime
import sqlite3

app = Flask(__name__)

def get_db():
    conn = sqlite3.connect("database.db")
    conn.row_factory = sqlite3.Row
    return conn

# =================================================
# rota dashboard
# =================================================
@app.route("/")
def index():
    return render_template("dashboard.html")
#=================================================
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

    # ---------------- CADASTRO ----------------
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
        estoque_minimo = request.form["estoque_minimo"]
        ncm = request.form["ncm"]
        tipo = request.form["tipo"]   # 🔥 NOVO CAMPO

        # 🔥 Se for serviço, não controla estoque
        if tipo == "servico":
            estoque = 0
            estoque_minimo = 0
        else:
            estoque = 0  # começa zerado até entrada manual depois

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

    # ---------------- BUSCA INTELIGENTE ----------------
    busca = request.args.get("busca", "").strip()

    if busca:
        cursor.execute("""
            SELECT * FROM produtos
            WHERE nome LIKE ?
            OR sku LIKE ?
            OR codigo_barras LIKE ?
            OR codigo_fabricante LIKE ?
        """, (
            f"%{busca}%",
            f"%{busca}%",
            f"%{busca}%",
            f"%{busca}%"
        ))
    else:
        cursor.execute("SELECT * FROM produtos")

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
        produtos=produtos
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
@app.route("/remover_item/<int:item_id>")
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
        SELECT id, nome, preco_venda AS preco, estoque, tipo
        FROM produtos
        WHERE
            nome LIKE ?
            OR sku LIKE ?
            OR codigo_barras LIKE ?
            OR codigo_fabricante LIKE ?
            OR substr(codigo_barras, -6) = ?
        
        UNION ALL
        
        SELECT id, nome, preco AS preco, 999 AS estoque, 'servico' AS tipo
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
            "tipo": r[4]
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
@app.route("/fechar_caixa", methods=["POST"])
def fechar_caixa():
    from datetime import datetime

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT id, valor_inicial FROM caixas WHERE status='ABERTO' LIMIT 1")
    caixa = cursor.fetchone()

    if not caixa:
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

    dinheiro = 0
    pix = 0
    cartao = 0

    for forma, valor in totais:
        if forma == "DINHEIRO":
            dinheiro = valor or 0
        elif forma == "PIX":
            pix = valor or 0
        elif forma == "CARTAO":
            cartao = valor or 0

    total_vendas = dinheiro + pix + cartao
    valor_final = valor_inicial + total_vendas

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

    return f"""
    <h2>📊 FECHAMENTO DO CAIXA</h2>
    <p>💰 Valor inicial: R$ {valor_inicial:.2f}</p>
    <p>💵 Dinheiro: R$ {dinheiro:.2f}</p>
    <p>📲 PIX: R$ {pix:.2f}</p>
    <p>💳 Cartão: R$ {cartao:.2f}</p>
    <hr>
    <p><strong>Total vendas: R$ {total_vendas:.2f}</strong></p>
    <h3>🏦 Valor esperado no caixa: R$ {valor_final:.2f}</h3>
    """
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

    conn.close()

    return render_template(
        "caixa.html",
        caixa=caixa,
        total_vendas=total_vendas,
        qtd_vendas=qtd_vendas
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
#=================================================
#rota para listar pedidos
#=================================================
@app.route("/caixa_pedidos")
def caixa_pedidos():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, total, data_abertura
        FROM movimentos
        WHERE status='PENDENTE'
        ORDER BY id DESC
    """)

    pedidos = cursor.fetchall()
    return render_template("caixa_pedidos.html", pedidos=pedidos)
#=================================================
#rota para receber pagamento
#=================================================
@app.route("/pagar_pedido/<int:id>", methods=["GET","POST"])
def pagar_pedido(id):
    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":
        forma = request.form["forma_pagamento"]
        bandeira = request.form.get("bandeira")

        # verificar caixa aberto
        cursor.execute("SELECT id FROM caixas WHERE status='ABERTO' LIMIT 1")
        caixa = cursor.fetchone()

        if not caixa:
            return "Nenhum caixa aberto!"

        caixa_id = caixa[0]

        # ATUALIZA MOVIMENTO E VINCULA AO CAIXA
        cursor.execute("""
            UPDATE movimentos
            SET status='PAGO',
                forma_pagamento=?,
                bandeira=?,
                caixa_id=?
            WHERE id=?
        """, (forma, bandeira, caixa_id, id))

        conn.commit()
        conn.close()

        return redirect(f"/imprimir_cupom/{id}")

    conn.close()
    return render_template("pagamento.html", pedido_id=id)
#=================================================
#rota gerar_pedido
#=================================================
@app.route("/gerar_pedido", methods=["POST"])
def gerar_pedido():
    data = request.get_json()
    itens = data["itens"]

    conn = get_db()
    cursor = conn.cursor()

    from datetime import datetime
    data_abertura = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # cria pedido pendente
    cursor.execute("""
        INSERT INTO movimentos (tipo, data_abertura, status, total)
        VALUES ('VENDA', ?, 'PENDENTE', 0)
    """, (data_abertura,))

    movimento_id = cursor.lastrowid
    total = 0

    for item in itens:
        subtotal = item["preco"] * item["quantidade"]
        total += subtotal

        cursor.execute("""
            INSERT INTO itens_movimento
            (movimento_id, produto_id, quantidade, preco_unitario, subtotal)
            VALUES (?, ?, ?, ?, ?)
        """, (
            movimento_id,
            item["id"],
            item["quantidade"],
            item["preco"],
            subtotal
        ))

        cursor.execute("""
            UPDATE produtos
            SET estoque = estoque - ?
            WHERE id = ?
        """, (item["quantidade"], item["id"]))

    cursor.execute("""
        UPDATE movimentos SET total=? WHERE id=?
    """, (total, movimento_id))

    conn.commit()

    return jsonify({"pedido_id": movimento_id})
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

    return {"total": total, "vendas": vendas} 
# ================================================
#rota que retorna pedidos em JSON
#=================================================
@app.route("/pedidos_pendentes")
def pedidos_pendentes():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, total
        FROM movimentos
        WHERE status='PENDENTE'
        ORDER BY id DESC
    """)

    pedidos = cursor.fetchall()

    lista = [
        {"id": p["id"], "total": p["total"]}
        for p in pedidos
    ]

    return {"pedidos": lista}
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

    return render_template("ver_pedido.html", pedido=pedido, itens=itens)
# ================================================
#rota gerar_pedido
#=================================================
@app.route("/imprimir_cupom/<int:id>")
def imprimir_cupom(id):
    from datetime import datetime

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM movimentos WHERE id=?", (id,))
    pedido = cursor.fetchone()

    cursor.execute("""
        SELECT p.nome, im.quantidade, im.preco_unitario, im.subtotal
        FROM itens_movimento im
        JOIN produtos p ON im.produto_id = p.id
        WHERE im.movimento_id=?
    """, (id,))
    itens = cursor.fetchall()

    data = datetime.now().strftime("%d/%m/%Y %H:%M")

    return render_template("cupom.html",
                           pedido=pedido,
                           itens=itens,
                           data=data)
# ================================================
#rota cadastro funcionarios
#=================================================
@app.route("/funcionarios", methods=["GET","POST"])
def funcionarios():

    conn = get_db()
    cursor = conn.cursor()

    if request.method == "POST":

        nome = request.form["nome"]
        cpf = request.form["cpf"]
        telefone = request.form["telefone"]
        whatsapp = request.form["whatsapp"]
        email = request.form["email"]
        cargo = request.form["cargo"]
        funcao = request.form["funcao"]
        comissao = request.form["comissao"]
        endereco = request.form["endereco"]

        cursor.execute("""
        INSERT INTO funcionarios
        (nome,cpf,telefone,whatsapp,email,cargo,funcao,comissao,endereco)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,(nome,cpf,telefone,whatsapp,email,cargo,funcao,comissao,endereco))

        conn.commit()

    cursor.execute("SELECT * FROM funcionarios")
    lista = cursor.fetchall()

    conn.close()

    return render_template("funcionarios.html", funcionarios=lista)
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
@app.route("/excluir_cliente/<int:id>")
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

    conn.commit()

    # pegar ID da OS criada
    os_id = cursor.lastrowid

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

    cursor.execute("""
    SELECT
        os.id,
        os.servico_nome,
        os.data_agendamento,
        os.hora,
        c.nome AS cliente,
        f.nome AS tecnico
    FROM ordem_servico os
    LEFT JOIN clientes c ON os.cliente_id = c.id
    LEFT JOIN funcionarios f ON os.tecnico_id = f.id
    ORDER BY os.data_agendamento, os.hora
    """)

    agenda = cursor.fetchall()

    conn.close()

    dias = {}

    for os in agenda:
        data = os["data_agendamento"]

        if data not in dias:
            dias[data] = []

        dias[data].append(os)

    return render_template("agenda_semana.html", dias=dias)
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

    cursor.execute("""
    SELECT os.*, c.nome, c.telefone, c.endereco, c.numero, c.bairro
    FROM ordem_servico os
    LEFT JOIN clientes c ON os.cliente_id = c.id
    WHERE os.id = ?
    """, (id,))

    ordem = cursor.fetchone()

    conn.close()

    return render_template("ver_ordem.html", ordem=ordem)
# ================================================
#rota gerar_pedido
#=================================================
if __name__ == "__main__":
    criar_tabelas()
    app.run(host="0.0.0.0", port=5000, debug=True)
