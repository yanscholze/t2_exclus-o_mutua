# T2 · Exclusão mútua com servidor central

Instituto Federal Catarinense, Câmpus São Bento do Sul  
Sistemas Distribuídos · Engenharia de Computação · Prof. Lucas Dominguez Cordeiro

**Integrante** — Yan Augusto Scholze

O servidor Python coordena o acesso a **R1** e **R2**. Três processos do mesmo cliente JavaScript, executado no Node.js, usam HTTP com JSON (REST). Se um pedido entra na fila, a concessão chega depois por **Server-Sent Events (SSE)**.

## Como executar

Use **Python 3.10 ou superior** e **Node.js 18 ou superior**. O projeto usa as bibliotecas padrão e pode ser executado diretamente. Abra quatro terminais na pasta que contém este README e rode um comando em cada um, na ordem abaixo.

| Terminal | Comando |
| --- | --- |
| Coordenador | `python3 servidor.py` |
| Cliente A | `node cliente.mjs ClienteA` |
| Cliente B | `node cliente.mjs ClienteB` |
| Cliente C | `node cliente.mjs ClienteC` |

No Windows, use `py -3 servidor.py` se o comando `python3` estiver indisponível.

No terminal de qualquer cliente, digite `pedir R1`, `pedir R2`, `liberar R1`, `liberar R2`, `estado`, `ajuda` ou `sair`. A seção crítica é simulada do registro **ENTROU** até o comando de liberação. A consulta **estado** exibe o dono e a fila de cada recurso sob demanda.

### Rede do laboratório

No computador servidor, execute `python3 servidor.py --host 0.0.0.0 --porta 8000`. Nos terminais dos clientes, acrescente à linha de comando o endereço do servidor. Por exemplo, `node cliente.mjs ClienteA http://192.168.1.10:8000`. Use o IP real do servidor e a porta escolhida. Apenas o servidor abre uma porta para conexões de entrada.

## Contrato REST

As requisições POST usam `Content-Type: application/json; charset=utf-8`. O campo ou parâmetro `cliente` aceita `ClienteA`, `ClienteB` e `ClienteC`. Os recursos são identificados por `R1` e `R2`. As respostas de erro usam `{"erro":"mensagem"}`.

| Verbo | Rota | Corpo JSON | Respostas |
| --- | --- | --- | --- |
| GET | `/eventos?cliente=ClienteA` | — | **200** `text/event-stream`. Evento `pronto` com `cliente`, seguido de eventos `concedido` com `evento`, `cliente`, `recurso` e `token`. **400** cliente inválido. **409** já existe um fluxo desse cliente. |
| GET | `/estado?cliente=ClienteA` | — | **200** `{"recursos":{"R1":{"dono":null,"fila":[]},"R2":{"dono":null,"fila":[]}}}` quando livres. **400** cliente inválido. |
| POST | `/pedidos` | `{"cliente":"ClienteA","recurso":"R1"}` | **200** `resultado` igual a `concedido` ou `ja_dono`, com `cliente`, `recurso` e `token`. A concessão imediata inclui `evento`. **202** `em_espera`, com `cliente`, `recurso` e `posicao` a partir de 1. **400**, **404**, **409** ou **415** em caso de erro. |
| POST | `/liberacoes` | `{"cliente":"ClienteA","recurso":"R1","token":1}` | **200** `{"resultado":"liberado","cliente":"ClienteA","recurso":"R1","novo_dono":null}` quando a fila está vazia. Com fila, `novo_dono` é o nome do primeiro da fila. **400**, **403**, **404**, **409** ou **415** em caso de erro. |

Erros nas operações POST

- **400** para JSON malformado, campos inválidos ou token com tipo inválido.
- **403** para liberação por quem está fora da posse do recurso. A checagem do dono vem antes da checagem do token.
- **404** para recurso desconhecido.
- **409** para conflito com a política contra impasse, tentativa de entrar na fila sem canal SSE ou liberação pelo dono com token ausente ou incorreto.
- **415** se o corpo for enviado com outro tipo de conteúdo.

Uma tentativa de liberação pelo menu sem token local envia `null`. Uma tentativa do dono com o token de uma concessão antiga é rejeitada. O número do token identifica uma concessão e o contrato usa os nomes como identidade, conforme o cenário do trabalho.

Exemplo com ferramenta genérica

```bash
curl -i 'http://127.0.0.1:8000/estado?cliente=ClienteA'
```

Exemplo de evento SSE após repasse

```text
event: concedido
data: {"evento":"concedido","cliente":"ClienteB","recurso":"R1","token":2}

```

O terminal inicia o SSE e aguarda o evento `pronto` antes de exibir o menu. Um cliente que obtém concessão imediata recebe o token pela resposta do pedido. Um cliente em fila recebe o token pelo SSE. O servidor envia comentários de manutenção pelo fluxo aberto e o cliente os ignora.

## Exclusão mútua e impasse

- **Segurança.** Cada recurso mantém um único dono. O servidor altera e consulta o estado sob o mesmo lock. A saída da seção crítica ocorre no cliente antes de enviar a devolução do token.
- **Ordenação.** A fila é FIFO pela ordem em que o servidor registra os pedidos sob o lock. A concessão e o repasse são atômicos. Repetir o pedido preserva a posse ou a posição.
- **Subsistência.** Com processos e comunicação operantes e cada dono devolvendo o token após usar o recurso, todo pedido aceito em fila chega à frente. Novos pedidos entram no fim.
- **Política de um recurso por cliente.** Quem possui ou espera um recurso recebe **409** ao pedir o outro. Assim, nenhum cliente fica segurando um recurso enquanto espera pelo outro. Dois clientes distintos ainda podem usar R1 e R2 simultaneamente.
- **Assincronia.** Cada requisição tem sua thread. A thread da liberação atualiza o estado, solta o lock e coloca o evento numa caixa de notificações. A thread da conexão SSE envia o evento com `flush`. O menu do cliente e a leitura do fluxo são independentes.

O cenário assume clientes cooperativos e conexões ativas durante o uso. Se um dono encerrar o processo ou perder a conexão antes de liberar, a execução atual pode ficar com um token retido. Nesse caso, para reiniciar a demonstração, encerre os clientes e reinicie o servidor.

## Aproveitamento das atividades

O código foi escrito para o T2 a partir dos requisitos do enunciado. Da Atividade 06 foram aproveitados os conceitos de token, fila e notificação. Da Atividade 10 foi aproveitada a ideia de interoperabilidade por HTTP com SSE. O T2 repassa a posse antes de avisar o próximo cliente.
