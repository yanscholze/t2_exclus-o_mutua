// Cliente de terminal do T2. Usa apenas APIs padrão do Node.js.
import { createInterface } from 'node:readline';
import { Readable } from 'node:stream';

const [cliente, endereco = 'http://127.0.0.1:8000'] = process.argv.slice(2);
const nomes = ['ClienteA', 'ClienteB', 'ClienteC'];
const recursos = ['R1', 'R2'];

function log(tipo, mensagem) {
    const hora = new Date().toLocaleTimeString('pt-BR', { hour12: false });
    console.log(`[${hora}] [${cliente}] [${tipo}] ${mensagem}`);
}

async function main() {
    if (!nomes.includes(cliente)) {
        console.error('Uso: node cliente.mjs ClienteA|ClienteB|ClienteC [http://host:porta]');
        process.exitCode = 1;
        return;
    }

    const servidor = new URL(endereco);
    if (servidor.protocol !== 'http:') {
        throw new Error('O endereço do coordenador deve começar com http://');
    }

    const consulta = `?cliente=${encodeURIComponent(cliente)}`;
    const cancelar = new AbortController();
    const tokens = new Map();
    const aguardando = new Set();
    let ultimoTokenVisto = 0;
    let menu;

    // Os tokens locais representam somente a SC simulada deste processo.
    // O servidor mantém o estado oficial. Um evento duplicado ou atrasado é ignorado.
    function entrarNaSC(recurso, token) {
        if (!recursos.includes(recurso) || !Number.isSafeInteger(token) || token <= ultimoTokenVisto) {
            return;
        }
        ultimoTokenVisto = token;
        aguardando.delete(recurso);
        tokens.set(recurso, token);
        log('SC', `ENTROU em ${recurso} com token ${token}. Digite "liberar ${recurso}" ao terminar.`);
    }

    // Envia uma operação solicitada no menu e registra a resposta recebida.
    async function requisitar(metodo, rota, dados) {
        log('ENVIO', `${metodo} ${rota}${dados ? ` ${JSON.stringify(dados)}` : ''}`);
        const opcoes = { method: metodo, signal: AbortSignal.timeout(5000) };
        if (dados) {
            opcoes.headers = { 'Content-Type': 'application/json; charset=utf-8' };
            opcoes.body = JSON.stringify(dados);
        }
        const resposta = await fetch(new URL(rota, servidor), opcoes);
        const corpo = await resposta.json();
        log('RESPOSTA', `HTTP ${resposta.status} ${JSON.stringify(corpo)}`);
        return { sucesso: resposta.ok, corpo };
    }

    // Cada evento deste servidor tem uma linha data. Os comentários SSE são ignorados.
    async function* lerEventos(resposta) {
        const linhas = createInterface({ input: Readable.fromWeb(resposta.body) });
        let evento;
        let dados;
        for await (const linha of linhas) {
            if (linha.startsWith('event: ')) {
                evento = linha.slice(7);
            } else if (linha.startsWith('data: ')) {
                dados = JSON.parse(linha.slice(6));
            } else if (linha === '' && evento && dados) {
                yield { evento, dados };
                evento = undefined;
                dados = undefined;
            }
        }
    }

    // Aguarda eventos em paralelo com a leitura do teclado, sem repetir GET.
    async function receberConcessoes(eventos) {
        try {
            for await (const { evento, dados } of eventos) {
                if (evento === 'concedido' && dados.cliente === cliente) {
                    log('SSE', JSON.stringify(dados));
                    entrarNaSC(dados.recurso, dados.token);
                    if (menu.terminal) {
                        menu.prompt(true);
                    }
                }
            }
            if (!cancelar.signal.aborted) {
                throw new Error('Fluxo SSE encerrado');
            }
        } catch (erro) {
            if (!cancelar.signal.aborted) {
                log('ERRO', erro.message);
                menu.close();
                process.exitCode = 1;
            }
        }
    }

    function ajuda() {
        console.log('Comandos: pedir R1 | pedir R2 | liberar R1 | liberar R2 | estado | ajuda | sair');
    }

    // Processa comandos de forma sequencial, enquanto o fluxo SSE permanece ativo.
    async function executar(linha) {
        const [acao, argumento, extra] = linha.trim().split(/\s+/);
        const recurso = argumento?.toUpperCase();
        if (!acao) {
            return;
        }
        if (acao === 'estado' && !argumento) {
            const { sucesso, corpo } = await requisitar('GET', `/estado${consulta}`);
            if (sucesso) {
                for (const [nome, estado] of Object.entries(corpo.recursos)) {
                    console.log(`${nome}  dono: ${estado.dono ?? 'livre'}  |  fila: ${estado.fila.join(' → ') || 'vazia'}`);
                }
            }
        } else if (acao === 'pedir' && recursos.includes(recurso) && !extra) {
            const { sucesso, corpo } = await requisitar('POST', '/pedidos', { cliente, recurso });
            if (sucesso && corpo.resultado === 'em_espera') {
                // O SSE pode chegar antes desta resposta.
                if (!tokens.has(recurso)) {
                    aguardando.add(recurso);
                    log('FILA', `Esperando ${recurso}, posição ${corpo.posicao}. Menu disponível.`);
                }
            } else if (sucesso) {
                entrarNaSC(recurso, corpo.token);
            }
        } else if (acao === 'liberar' && recursos.includes(recurso) && !extra) {
            const token = tokens.get(recurso) ?? null;
            if (token !== null) {
                tokens.delete(recurso);
                log('SC', `SAIU de ${recurso}. Devolvendo o token ${token}.`);
            }
            // O servidor valida inclusive as tentativas de quem não tem o token.
            await requisitar('POST', '/liberacoes', { cliente, recurso, token });
        } else if (acao === 'sair' && !argumento) {
            if (!tokens.size && !aguardando.size) {
                return false;
            }
            log('MENU', 'Aguarde a concessão em fila e devolva o token antes de sair.');
        } else {
            ajuda();
        }
    }

    try {
        log('ENVIO', `GET /eventos${consulta}`);
        const prazo = setTimeout(() => cancelar.abort(), 5000);
        let eventos;
        try {
            const resposta = await fetch(new URL(`/eventos${consulta}`, servidor), { signal: cancelar.signal });
            if (!resposta.ok) {
                const corpo = await resposta.json();
                log('RESPOSTA', `HTTP ${resposta.status} ${JSON.stringify(corpo)}`);
                throw new Error(corpo.erro);
            }
            eventos = lerEventos(resposta);
            const { value } = await eventos.next();
            if (value?.evento !== 'pronto') {
                throw new Error('O servidor não confirmou a abertura do fluxo SSE');
            }
            log('RESPOSTA', 'HTTP 200. Canal SSE aberto.');
        } finally {
            clearTimeout(prazo);
        }

        menu = createInterface({
            input: process.stdin, output: process.stdout, prompt: `${cliente}> `,
        });
        const recebimento = receberConcessoes(eventos);
        ajuda();
        menu.prompt();

        for await (const linha of menu) {
            try {
                if (await executar(linha) === false) {
                    break;
                }
            } catch (erro) {
                log('ERRO', erro.message);
            }
            if (!cancelar.signal.aborted && !menu.closed) {
                menu.prompt();
            }
        }
        menu.close();
        cancelar.abort();
        await recebimento;
    } finally {
        cancelar.abort();
        menu?.close();
    }
}

main().catch((erro) => {
    console.error(`Erro: ${erro.message}`);
    process.exitCode = 1;
});
