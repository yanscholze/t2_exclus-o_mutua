"""Coordenador central do T2. Usa apenas a biblioteca padrão do Python."""

import argparse
import json
import logging
from collections import deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Empty, Queue
from threading import Lock
from urllib.parse import parse_qs, urlsplit


CLIENTES = ("ClienteA", "ClienteB", "ClienteC")


class ErroHTTP(Exception):
    def __init__(self, codigo, mensagem):
        super().__init__(mensagem)
        self.codigo = codigo


@dataclass
class Recurso:
    dono: str | None = None
    token: int | None = None
    fila: deque = field(default_factory=deque)


class Coordenador:
    def __init__(self):
        self.recursos = {"R1": Recurso(), "R2": Recurso()}
        self.canais = {}
        self.ultimo_token = 0
        self.lock = Lock()

    def _recurso(self, nome):
        if nome not in self.recursos:
            raise ErroHTTP(404, f"Recurso inexistente: {nome}")
        return self.recursos[nome]

    def _conceder(self, nome, recurso, cliente):
        """Com o lock: atribui a posse e cria o token de uma nova concessão."""
        self.ultimo_token += 1
        recurso.dono = cliente
        recurso.token = self.ultimo_token
        return {
            "evento": "concedido", "cliente": cliente,
            "recurso": nome, "token": recurso.token,
        }

    def pedir(self, cliente, nome):
        """Concede, enfileira ou devolve o resultado do pedido anterior."""
        with self.lock:
            recurso = self._recurso(nome)
            base = {"cliente": cliente, "recurso": nome}
            if recurso.dono == cliente:
                return 200, {**base, "resultado": "ja_dono", "token": recurso.token}

            if cliente in recurso.fila:
                posicao = recurso.fila.index(cliente) + 1
                return 202, {**base, "resultado": "em_espera", "posicao": posicao}

            for outro_nome, outro in self.recursos.items():
                if outro_nome != nome and (outro.dono == cliente or cliente in outro.fila):
                    raise ErroHTTP(409, f"Política de um recurso por cliente: {cliente} já ocupa ou espera {outro_nome}")

            if recurso.dono is None:
                evento = self._conceder(nome, recurso, cliente)
                logging.info("CONCESSÃO %s para %s, token %s", nome, cliente, recurso.token)
                return 200, {**evento, "resultado": "concedido"}

            if cliente not in self.canais:
                raise ErroHTTP(409, "Abra o canal /eventos antes de entrar na fila")

            recurso.fila.append(cliente)
            logging.info("FILA %s: %s na posição %d", nome, cliente, len(recurso.fila))
            return 202, {**base, "resultado": "em_espera", "posicao": len(recurso.fila)}

    def liberar(self, cliente, nome, token):
        """Valida a posse e transfere o token ao primeiro da fila, atomicamente."""
        canal = None
        evento = None
        with self.lock:
            recurso = self._recurso(nome)
            if recurso.dono != cliente:
                raise ErroHTTP(403, f"{cliente} não é o dono de {nome}")
            if recurso.token != token:
                raise ErroHTTP(409, "Token ausente ou diferente da concessão atual")

            if recurso.fila:
                proximo = recurso.fila.popleft()
                evento = self._conceder(nome, recurso, proximo)
                canal = self.canais.get(proximo)
                logging.info("REPASSE %s: %s para %s, token %s", nome, cliente, proximo, recurso.token)
            else:
                recurso.dono = recurso.token = None
                logging.info("LIBERAÇÃO %s por %s; recurso livre", nome, cliente)
            novo_dono = recurso.dono

        # A thread do SSE envia o evento. Esta thread apenas o coloca no canal.
        if canal is not None:
            canal.put_nowait(evento)
        elif evento is not None:
            logging.warning("Canal de %s desconectado ao repassar %s", evento["cliente"], nome)

        return 200, {
            "resultado": "liberado", "cliente": cliente, "recurso": nome,
            "novo_dono": novo_dono,
        }

    def estado(self):
        """Copia dono e fila sob o mesmo lock das operações de escrita."""
        with self.lock:
            return {
                nome: {"dono": recurso.dono, "fila": list(recurso.fila)}
                for nome, recurso in self.recursos.items()
            }

    def abrir_canal(self, cliente):
        """Cria uma caixa de eventos para a conexão SSE do cliente."""
        with self.lock:
            if cliente in self.canais:
                raise ErroHTTP(409, f"Já existe um canal SSE para {cliente}")
            canal = Queue()
            self.canais[cliente] = canal
            return canal

    def fechar_canal(self, cliente):
        with self.lock:
            del self.canais[cliente]


class API(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # O log estruturado das operações fica em _registrar.
        pass

    def _registrar(self, cliente, recurso, codigo, detalhe):
        logging.info("%s %s cliente=%s recurso=%s HTTP %s %s",
                     self.command, self.path.split("?")[0], cliente, recurso, codigo, detalhe)

    def _responder(self, codigo, dados, permitido=None):
        corpo = json.dumps(dados, ensure_ascii=False).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(corpo)))
        if permitido is not None:
            self.send_header("Allow", permitido)
        self.end_headers()
        self.wfile.write(corpo)

    def _ler_json(self):
        """Lê apenas objetos JSON UTF-8 de tamanho limitado."""
        if self.headers.get_content_type() != "application/json":
            raise ErroHTTP(415, "Use Content-Type: application/json")
        try:
            tamanho = int(self.headers.get("Content-Length", "0"))
            if not 0 < tamanho <= 8192:
                raise ValueError
            dados = json.loads(self.rfile.read(tamanho).decode("utf-8"))
            if not isinstance(dados, dict):
                raise ValueError
        except (ValueError, RecursionError) as erro:
            raise ErroHTTP(400, "Envie um objeto JSON UTF-8 válido") from erro
        return dados

    def _enviar_evento(self, evento, dados):
        """Um quadro SSE tem nome, dados JSON e uma linha em branco."""
        quadro = f"event: {evento}\ndata: {json.dumps(dados, ensure_ascii=False)}\n\n"
        self.wfile.write(quadro.encode("utf-8"))
        self.wfile.flush()

    def _eventos(self, cliente):
        """A thread da conexão espera na caixa e escreve sem segurar o lock."""
        canal = self.server.coordenador.abrir_canal(cliente)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self._enviar_evento("pronto", {"cliente": cliente})
            self._registrar(cliente, "-", 200, "canal SSE aberto")
            while True:
                try:
                    dados = canal.get(timeout=10)
                except Empty:
                    # Comentário SSE. Mantém o fluxo ativo e detecta desconexões.
                    self.wfile.write(b": ativo\n\n")
                    self.wfile.flush()
                    continue
                self._enviar_evento("concedido", dados)
                logging.info("SSE enviado: %s recebeu %s, token %s",
                             cliente, dados["recurso"], dados["token"])
        except OSError:
            logging.info("Canal SSE desconectado: %s", cliente)
        finally:
            self.server.coordenador.fechar_canal(cliente)

    def _processar(self):
        """Valida a requisição, chama o coordenador e registra a resposta."""
        cliente, recurso = "-", "-"
        permitido = None
        coordenador = self.server.coordenador
        try:
            url = urlsplit(self.path)
            if self.command == "GET" and url.path in ("/estado", "/eventos"):
                valores = parse_qs(url.query).get("cliente", [])
                cliente = valores[0] if len(valores) == 1 else "-"
                if cliente not in CLIENTES:
                    raise ErroHTTP(400, "Informe ClienteA, ClienteB ou ClienteC")
                if url.path == "/eventos":
                    self._eventos(cliente)
                    return
                codigo, dados = 200, {"recursos": coordenador.estado()}
            elif self.command == "POST" and url.path in ("/pedidos", "/liberacoes"):
                corpo = self._ler_json()
                cliente, recurso = corpo.get("cliente"), corpo.get("recurso")
                if cliente not in CLIENTES or not isinstance(recurso, str) or not recurso:
                    raise ErroHTTP(400, "Informe um cliente válido e o nome do recurso")
                if url.path == "/pedidos":
                    codigo, dados = coordenador.pedir(cliente, recurso)
                else:
                    token = corpo.get("token")
                    if token is not None and (type(token) is not int or token < 1):
                        raise ErroHTTP(400, "O token deve ser um inteiro positivo ou null")
                    codigo, dados = coordenador.liberar(cliente, recurso, token)
            else:
                if url.path in ("/estado", "/eventos", "/pedidos", "/liberacoes"):
                    permitido = "GET" if url.path in ("/estado", "/eventos") else "POST"
                    raise ErroHTTP(405, "Método HTTP incorreto para esta rota")
                raise ErroHTTP(404, "Rota inexistente")
        except ErroHTTP as erro:
            codigo, dados = erro.codigo, {"erro": str(erro)}
        except ValueError:
            codigo, dados = 400, {"erro": "Endereço da requisição inválido"}

        self._registrar(cliente, recurso, codigo, dados)
        self._responder(codigo, dados, permitido)

    do_GET = _processar
    do_POST = _processar
    do_PUT = _processar
    do_PATCH = _processar
    do_DELETE = _processar

def main():
    parser = argparse.ArgumentParser(description="Coordenador do T2")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=8000)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)s] %(message)s",
                        datefmt="%H:%M:%S")
    with ThreadingHTTPServer((args.host, args.porta), API) as servidor:
        servidor.coordenador = Coordenador()
        host, porta = servidor.server_address
        logging.info("Coordenador em http://%s:%s com R1 e R2 livres", host, porta)
        try:
            servidor.serve_forever()
        except KeyboardInterrupt:
            logging.info("Servidor encerrado")


if __name__ == "__main__":
    main()
