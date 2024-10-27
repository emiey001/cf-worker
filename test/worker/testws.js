/**
 * This Cloudflare Worker handles incoming requests and manages VPN 
 * tunneling using the VLESS protocol. It processes query parameters 
 * to establish connections securely.
 * 
 * Original idea: https://github.com/zizifn/edgetunnel
 * Modified by: Ahmad Sysdev
 */

// import { ReadableStream, WebSocketPair, WritableStream } from '@cloudflare/workers-types';
import { connect } from 'cloudflare:sockets';
import pino, { P } from 'pino';

// Global variables
const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;
let userID = '63fc3853-3fd5-444c-8dca-d8579bec588e'; // python -c 'import uuid;print(uuid.uuid4())'
let proxy = '';
const logger = pino({
    transport: {
        target: 'pino-pretty',
        options: {
          colorize: true
        }
    }
})


// Main function
export default {
    /**
     * @param {import("@cloudflare/workers-types").Request} request
     * @param {{UUID: string, PROXY: string}} env
     * @param {import("@cloudflare/workers-types").ExecutionContext} ctx
     * @returns {Promise<Response>}
     */
    async fetch(request, env, ctx) {
        try {
            userID = env.UUID || userID;
            proxy = env.PROXY || proxy;
            const upgradeHeader = request.headers.get('Upgrade');
            if (!upgradeHeader || upgradeHeader !== 'websocket') {
                const url = new URL(request.url);
                switch (url.pathname) {
                    case '/':
                        return new Response(JSON.stringify(request.cf), { status: 200 });
                    case `/${userID}`: {
                        const config = getConfig(userID, request.headers.get('Host'));
                        return new Response(`${config}`, {
                            status: 200,
                            headers: {
                                "Content-Type": "text/plain;charset=utf-8",
                            }
                        })
                    }
                    default:
                        return new Response('Not found', { status: 404 });
                }
            }
            else {
                return await vlessOverWSHandler(request);
            }
        }
        catch (err) {
            /** @type {Error} */
            return new Response(err.toString());
        }
    }
}

/**
 * @param {import("@cloudflare/workers-types").Request} request
 */
async function vlessOverWSHandler(request) {
    /**
     * @type {import("@cloudflare/workers-types").WebSocket[]}
     */
    const webSocketPair = new WebSocketPair();
    const [client, webSocketServer] = Object.values(webSocketPair);
    webSocketServer.accept();
    const earlyDataHeader = request.headers.get('sec-websocket-proocol') || '';
    const readableWebSocketStream = makeReadableWebSocketStream(webSocketServer, earlyDataHeader);
    
    /**
     * @type {{ value: import("@cloudflare/workers-types").Socket || null }}
     */
    let remoteSocketWrapper = {
        value: null,
    };
    let UDPStreamWrite = null;
    let isDNS = false;

    // ws --> remote
    readableWebSocketStream.pipeTo(new WritableStream({
        async write(chunk, controller) {
            if (isDNS && UDPStreamWrite) {
                return UDPStreamWrite(chunk);
            }
            if (remoteSocketWrapper.value) {
                const writer = remoteSocketWrapper.value.writable.getWriter();
                await writer.write(chunk);
                writer.releaseLock();
                return;
            }

            const {
                hasError,
                message,
                remotePort = 443,
                remoteAddress = '',
                rawDataIndex,
                vlessVersion = new Uint8Array([0, 0]),
                isUDP,
            } = vlessHeaderParser(chunk, userID);
            console.log(isUDP)
            if (hasError) {
                throw new Error(message); // CF seems has bug, controller.error will not end stream.
            }
            // If UDP but port not DNS port, close it.
            if (isUDP) {
                if (remotePort === 53) {
                    isDNS = true;
                }
                else {
                    throw new Error('UDP proxy only enable for DNS which is port 53.');
                }
            }
            const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
            const rawClientData = chunk.slice(rawDataIndex);

            // TODO: support UDP here when cf runtime has UDP support
            if (isDNS) {
                const { UDPStreamWrite } = await handleUDPOutBound(webSocketServer, vlessResponseHeader);
                UDPStreamWrite(rawClientData);
                return;
            }
            handleTCPOutBound(remoteSocketWrapper, remoteAddress, remotePort, rawClientData, webSocketServer, vlessResponseHeader);
        },
        close() {
            logger.info(`readableWebSocketStream is closed.`);
        },
        abort(reason) {
            logger.error(`readableWebSocketStream is aborted due to ${reason}`);
        },
    }))
    .catch((err) => {
        logger.info(`readableWebSocketstream pipeTo has error: ${err.stack || err}`);
    });

    return new Response(null, {
        status: 101,
        webSocket: client,
    })
}

/**
 * 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocketServer
 * @param {string} earlyDataHeader for ws 0-RTT
 */
function makeReadableWebSocketStream(webSocketServer, earlyDataHeader) {
    let readableStreamCancel = false;
    console.log(`readableStreamCanel:`, readableStreamCancel)
    const stream = new ReadableStream({
        start(controller) {
            webSocketServer.addEventListener('message', (event) => {
                if (readableStreamCancel) {
                    return;
                }
                const message = event.data;
                controller.enqueue(message);
                console.log(message)
            });

            // The event means that the client closed the client -> server stream.
            // However, the server -> client stream is still open until you call close() on the server side.
            // The WebSocket protocol says that a separate close message must be sent in each direction to fully close the socket.
            webSocketServer.addEventListener('close', () => {
                // Client side send close, need to close server.
                // If stream is cancel, skip controller.close
                safeCloseWebSocket(webSocketServer);
                if (readableStreamCancel) {
                    return;
                }
                controller.close();
            });
            webSocketServer.addEventListener('error', (err) => {
                logger.error(err);
                controller.error(err);
            });
            
            // For WebSocket 0-RTT
            let earlyData = null;
            if (earlyDataHeader) {
                try {
                    // Use modified base64 for URL RFC 4648 which js atob not support
                    let base64Str = earlyDataHeader.replace(/-/g, '+').replace(/_/g, '/');
                    const decode = atob(base64Str);
                    earlyData = Uint8Array.from(decode, (c) => c.charCodeAt(0)).buffer;
                    controller.enqueue(earlyData);
                    console.log(earlyData)
                }
                catch (err) {
                    /**
                     * @type {Error}
                     */
                    logger.error(err);
                    controller.error(err);
                }
            }
        },

        pull(controller) {
            // If WebSocket can stop read if stream is full, we can implement backpressure
            // https://streams.spec.whatwg.org/#example-rs-push-backpressure
        },

        cancel(reason) {
            /**
             * 1. Pipe WriteableStream has error, this cancel will called, so WebSocket handle server close into here
             * 2. if readableStream is cancel, all controller.close/enqueue need skip but from testing controller.error still work even if readableStream is cancel.
             */
            if (readableStreamCancel) {
                return;
            }
            logger(`ReadableStream was cancelled, due to ${reason}`);
            readableStreamCancel = true;
            safeCloseWebSocket(webSocketServer);
        }
    });

    return stream;
}

/**
 * Normally, WebSocket will not has exceptions when close.
 * @param {import("@cloudflare/workers-types").WebSocket} socket
 */
function safeCloseWebSocket(socket) {
    try {
        if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
            socket.close()
        }
    }
    catch (err) {
        /**
         * @type {Error}
         */
        logger.error(err);
    }
}

/**
 * @param {ArrayBuffer} buffer
 * @param {string} userID
 * @returns {Object}
 */
function vlessHeaderParser(buffer, userID) {
    if (buffer.byteLength < 24) {
        return {
            hasError: true,
            message: 'Invalid data.',
        };
    }
    const version = new Uint8Array(buffer.slice(0, 1));
    if (stringify(new Uint8Array(buffer.slice(1, 17))) !== userID) {
        return {
            hasError: true,
            message: 'Invalid user.',
        };
    }

    const optLength = new Uint8Array(buffer.slice(17, 18))[0];
    // Skip opt for now

    const protocol = new Uint8Array(
        buffer.slice(18 + optLength, 18 + optLength + 1)
    )[0];

    // 0x01 TCP
    // 0X02 UDP
    // 0X03 MUX
    if (protocol === 1) {
        // Default
    }
    else if (protocol === 2) {
        let isUDP = true;
    }
    else {
        // Mux is not available for now.
        return {
            hasError: true,
            message: `This protocol (${protocol}) is not supported for now.`,
        };
    }
    const portIndex = 18 + optLength + 1;
    const portBuffer = buffer.slice(portIndex, portIndex + 2);
    console.log(portBuffer)
    // Port is big-endian in raw data etc 80 == 0x005d
    const remotePort = new DataView(portBuffer).getUint16(0);

    let addressIndex = portIndex + 2;
    const addressBuffer = new Uint8Array(
        buffer.slice(addressIndex, addressIndex + 1)
    );

    // 1 -> IPv4 addressLength = 4
    // 2 -> Domain name addressLength = addressBuffer[1]
    // 3 -> IPv6 addressLength = 16
    const addressType = addressBuffer[0];
    let addressLength = 0;
    let addressValueIndex = addressIndex + 1;
    let addressValue = '';
    switch (addressType) {
        case 1:
            addressLength = 4;
            addressValue = new Uint8Array(
                buffer.slice(addressValueIndex, addressValueIndex + addressLength)
            ).join('.');
            break;
        case 2:
            addressLength = new Uint8Array(
                buffer.slice(addressValueIndex, addressValueIndex + 1)
            )[0];
            addressValueIndex += 1;
            addressValue = new TextDecoder().decode(
                buffer.slice(addressValueIndex, addressValueIndex + addressLength)
            );
            break;
        case 3:
            addressLength = 16;
            const dataView = new DataView(
                buffer.slice(addressValueIndex, addressValueIndex + addressLength)
            );
            // 2001:0db8:85a3:0000:0000:8a2e:0370:7334
            const ipv6 = []
            for (let i = 0; i < 8;i++) {
                ipv6.push(dataView.getUint16(i * 2).toString(16));
            }
            addressValue = ipv6.join(':');
            // Seems no need add [] for ipv6.
            break;
        default:
            return {
                hasError: true,
                message: `Invalid type of address '${addressType}'`,
            };
    }
    if (!addressValue) {
        return {
            hasError: true,
            message: `Address value is empty, addressType is '${addressType}'.`,
        };
    }
    return {
        hasError: false,
        remoteAddress: addressValue,
        addressType,
        remotePort,
        rawDataIndex: addressValueIndex + addressLength,
        vlessVersion: version,
        isUDP,
    };
}

/**
 * UDP handler.
 * 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocketServer
 * @param {ArrayBuffer} vlessResponseHeader
 * @returns {Object}
 */
async function handleUDPOutBound(webSocketServer, vlessResponseHeader) {
    let vlessHeaderIsSent = false;
    const stream = new TransformStream({
        start(controller) {
            // Do nothing
        },
        transform(chunk, controller) {
            // UDP message 2 byte is the length of UDP data.
            // TODO: this should have bug, because maybe UDP chunk can be in two WebSocket message
            for (let index = 0; index < chunk.byteLength;) {
                const lengthBuffer = chunk.slice(index, index + 2);
                const UDPPacketLength = new DataView(lengthBuffer).getUint16(0);
                const UDPData = new Uint8Array(
                    chunk.slice(index + 2, index + 2 + UDPPacketLength)
                );
                index = index + 2 + UDPPacketLength;
                controller.enqueue(UDPData);
            }
        },
        flush(controller) {
            // Flush.
        }
    });

    // Only handle DNS UDP for now.
    stream.readable.pipeTo(new WritableStream({
        async write(chunk) {
            const response = await fetch("https://1.1.1.1/dns-query",
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/dns-message",
                    },
                    body: chunk,
                }
            );
            const DNSQueryResult = await response.arrayBuffer();
            const UDPSize = DNSQueryResult.byteLength;
            console.log([...new Uint8Array(DNSQueryResult)].map((x) => x.toString(16)));
            const UDPSizeBuffer = new Uint8Array([(UDPSize >> 8) & 0xff, UDPSize & 0xff]);
            if (webSocketServer.readyState === WS_READY_STATE_OPEN) {
                logger.info(`DOH success and DNS message length is ${UDPSize}`);
                if (vlessHeaderIsSent) {
                    webSocketServer.send(await new Blob([UDPSizeBuffer, DNSQueryResult]).arrayBuffer());
                }
                else {
                    webSocketServer.send(await new Blob([vlessResponseHeader, UDPSizeBuffer, DNSQueryResult]).arrayBuffer());
                    vlessHeaderIsSent = true;
                }
            }
        }
    })).catch((x) => {
        // Got an error here pal.
        logger.err(x);
    });
    const writer = stream.writable.getWriter();
    return {
        /**
         * @param {Uint8Array} chunk
         */
        UDPStreamWrite(chunk) {
            writer.write(chunk);
        }
    };
}

/**
 * @param {Uint8Array} arr
 * @param {number} offset
 * @returns {string}
 */
function stringify(arr, offset = 0) {
    const byteToHex = [];
    for (let i = 0;i < 256;i++) {
        byteToHex.push((i + 256).toString(16).slice(1));
    }
    const parts = [];
    // Loop through 16 bytes starting from the given offset
    for (let i = 0; i < 16;i++) {
        parts.push(byteToHex[arr[offset + i]]);
    }
    return (
        parts.slice(0, 4).join('') + '-' +
        parts.slice(4, 6).join('') + '-' +
        parts.slice(6, 8).join('') + '-' +
        parts.slice(8, 10).join('') + '-' +
        parts.slice(10, 16).join('')
    ).toLocaleLowerCase();
}

/**
 * @param {import("@cloudflare/workers-types").Socket} remoteSocketWrapper The remote socket.
 * @param {string} remoteAddress The remote address.
 * @param {number} remotePort The remote port to connect to.
 * @param {Uint8Array} rawClientData The raw client data to write.
 * @param {import("@cloudflare/workers-types").WebSocket} webSocketServer the WebSocket to pass the remote socket to.
 * @param {Uint8Array} vlessResponseHeader The vless response header.
 * @returns {Promise<void>} the remote socket.
 */
async function handleTCPOutBound(remoteSocketWrapper, remoteAddress, remotePort, rawClientData, webSocketServer, vlessResponseHeader) {
    /**
     * 
     * @param {string} address The remote address. 
     * @param {number} port The remote port to connect to.
     * @returns {import("@cloudflare/workers-types").Socket}
     */
    async function connectAndWrite(address, port) {
        /**
         * @type {import("@cloudflare/workers-types").Socket}
         */
        const TCPSocket = connect({
            hostname: address,
            port: port,
        });
        remoteSocketWrapper.value = TCPSocket;
        logger.info(`Connected to ${address} with port ${port}.`);
        const writer = TCPSocket.writable.getWriter();
        await writer.write(rawClientData); // First write, normally TLS client hello
        writer.releaseLock();
        return TCPSocket
    }

    // If the CF connect TCP socket have no incoming data, we retry to redirect IP
    async function retry() {
        const TCPSocket = await connectAndWrite(proxy || remoteAddress, remotePort);
        // No matter retry success or not, close the WebSocket connection.
        TCPSocket.closed.catch(error => {
            logger.error(`Retry TCPSocket closed error`, error);
        }).finally(() => {
            safeCloseWebSocket(webSocketServer);
        });
        remoteSocketToWS(TCPSocket, webSocketServer, vlessResponseHeader, null);
    }

    const TCPSocket = await connectAndWrite(remoteAddress, remotePort);
    
    // When remote socket is ready, pass to the WebSocket
    // Remote --> Websocket
    remoteSocketToWS(TCPSocket, webSocketServer, vlessResponseHeader, retry);
}

/**
 * 
 * @param {import("@cloudflare/workers-types").Socket} remoteSocket 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocketServer 
 * @param {ArrayBuffer} vlessResponseHeader 
 * @param {(() => Promise<void> ) | null} retry 
 * @returns {Promise<void>}
 */
async function remoteSocketToWS(remoteSocket, webSocketServer, vlessResponseHeader, retry) {
    // Remote --> Websocket
    let remoteChunkCount = 0;
    let chunks = [];
    /**
     * @type {ArrayBuffer || null}
     */
    let hasIncomingData = false; // Check if remote socket has incoming data.
    await remoteSocket.readable.pipeTo(new WritableStream({
        start() {
            // Nothing here pal.
        },
        /**
         * 
         * @param {Uint8Array} chunk 
         * @param {*} controller 
         */
        async write(chunk, controller) {
            hasIncomingData = true;
            if (webSocketServer.readyState !== WS_READY_STATE_OPEN) {
                controller.error('webSocketServer.readyState is not open.');
            }
            if (vlessResponseHeader) {
                webSocketServer.send(await new Blob([vlessResponseHeader, chunk]).arrayBuffer());
                vlessResponseHeader = null;
            }
            else {
                webSocketServer.send(chunk);
            }
        },
        close() {
            logger.info(`remoteConnection!.readable is close with hasIncomingData is ${hasIncomingData}`);
        },
        abort(reason) {
            logger.error(`remoteConnection!.readable abort due to ${reason}`);
        },
    }))
    .catch(error => {
        logger.error(`remoteSocketToWS has exception: ${error.stack || error}`);
        safeCloseWebSocket(webSocketServer);
    });

    // Seems is CF connect socket have error
    // 1. socket.closed will have error
    // 2. socket.readable will be close without any data coming
    if (!hasIncomingData && retry) {
        logger.info('Retry');
        retry();
    }

}

/**
 * 
 * @param {string} userID 
 * @param {string | null} hostname 
 * @returns {string}
 */
function getConfig(userID, hostname) {
    const clipboard = `vless://${userID}\u0040${hostname}?encryption=none&security=tls&sni=${hostname}&fp=randomized&type=ws&host=${hostname}&path=%2F%3Fed%3D2048#${hostname}`;
    return clipboard;
}