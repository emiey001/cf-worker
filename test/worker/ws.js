// import { WebSocketPair } from "@cloudflare/workers-types";

import pino, { P } from 'pino';

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

export default {
    /**
     * 
     * @param {import("@cloudflare/workers-types").Request} request 
     * @param {Object} env 
     * @param {import("@cloudflare/workers-types").ExecutionContext} ctx 
     * @returns 
     */
    async fetch(request, env, ctx) {
        const upgradeHeader = request.headers.get('Upgrade');
        if (!upgradeHeader || upgradeHeader !== 'websocket') {
            return new Response('Not websocket request.', { status: 200 });
        }
        /**
         * @type {import("@cloudflare/workers-types").WebSocket[]} websocket
         */
        const ws = new WebSocketPair();
        const [client, webSocket] = Object.values(ws);
        webSocket.accept();
        const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
        const readable = new ReadableStream({
            start(controller) {
                webSocket.addEventListener('message', (event) => {
                    console.log('Got messsage', event.data);
                    const message = event.data;
                    controller.enqueue(message);
                });

                webSocket.addEventListener('close', () => {
                    safeCloseWebSocket(webSocket);
                    controller.close();
                });

                webSocket.addEventListener('error', (error) => {
                    console.error('webSocket has error:', error);
                    controller.error(error);
                });

                if (earlyDataHeader) {
                    try {
                        const decode = atob(earlyDataHeader.replace(/-/g, '+').replace(/_/g, '/'));
                        console.log(decode);
                        const buff = Uint8Array.from(decode, (c) => c.charCodeAt(0));
                        controller.enqueue(buff.buffer)
                    } catch (error) {
                        controller.error(error);
                    }
                }
            },
            async pull(controller) {
                console.log('ReadableStream was pulled.')
            },
            cancel(reason) {
                console.log('ReadableStream was cancelled. Reason:', reason);
            },
        });

        /**
         * @type {{ value: import("@cloudflare/workers-types").Socket }}
         */
        let remoteSocketWrapper = {
            value: null,
        };
        let UDPStreamWrite = null;
        let isDNS = false;

        const writable = new WritableStream({
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
                    remotePort,
                    remoteAddress,
                    rawDataIndex = '',
                    vlessVersion = new Uint8Array([0, 0]),
                    isUDP
                } = vlessHeaderParser(chunk, userID);
                if (hasError) {
                    throw new Error(message);
                }
                if (isUDP) {
                    console.log(`UDP request from ${remoteAddress}`)
                    if (remotePort === 53) {
                        console.log(`DNS query from ${remoteAddress}: ${message}`);
                        isDNS = true;
                    } else {
                        throw new Error(`UDP protocol is not supported for now.`);
                    }

                    const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
                    const rawClientData = chunk.slice(rawDataIndex);

                    if (isDNS) {
                        UDPStreamWrite = (await handleUDPOutBound(webSocket, vlessResponseHeader)).UDPStreamWrite;
                        UDPStreamWrite(rawClientData);
                        return;
                    }
                }
            },
            close() {
                console.log('WritableStream was closed.')
            },
            abort() {
                console.log('WritableStream was aborted.')
            }
        });

        readable.pipeTo(writable).catch((error) => {
            console.log('pipeTo error', error);
            webSocket.close();
        });

        return new Response(null, {
            status: 101,
            webSocket: client,
        })
    }
}

/**
 * Normally, WebSocket will not have exceptions when close.
 * @param {import("@cloudflare/workers-types").WebSocket} socket 
 */
function safeCloseWebSocket(socket) {
    try {
        if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
            socket.close();
        }
    } catch (error) {
        console.error('safeCloseWebSocket error:', error);
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
    // Port is big-endian in raw data etc 80 == 0x005d
    console.log(portBuffer);
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
 * 
 * @param {import("@cloudflare/workers-types").WebSocket} websocket 
 * @param {ArrayBuffer} vlessResponseHeader 
 */
async function handleUDPOutBound(websocket, vlessResponseHeader) {
    let vlessHeaderIsSent = false;
    const transfrom = new TransformStream({
        start(controller) {
            // 
        },
        transform(chunk, controller) {
            for (let index = 0; index < chunk.byteLength;) {
                const lengthBuffer = chunk.slice(index, index + 2);
                const UDPPacketLength = new DataView(lengthBuffer);
                const UDPData = new Uint8Array(
                    chunk.slice(index + 2, index + 2 + UDPPacketLength)
                );
                index = index + 2 + UDPPacketLength;
                controller.enqueue(UDPData);
            }
        },
        flush(controller) {
            // 
        },
    });

    transfrom.readable.pipeTo(new WritableStream({
        async write(chunk) {
            console.log('Quering DNS to 1.1.1.1...')
            const response = await fetch('https://1.1.1.1/dns-query',
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/dns-message",
                    },
                    body: chunk
                }
            );
            const DNSQueryResult = await response.arrayBuffer();
            const UDPSize = DNSQueryResult.byteLength;
            const UDPSizeBuffer = new Uint8Array([(UDPSize >> 8) & 0xff, UDPSize & 0xff]);
            if (websocket.readyState === WS_READY_STATE_OPEN) {
                console.log(`DOH success and DNS message length is ${UDPSize}`);
                if (vlessHeaderIsSent) {
                    websocket.send(await new Blob([UDPSizeBuffer, DNSQueryResult]).arrayBuffer());
                } else {
                    websocket.send(await new Blob([vlessResponseHeader, UDPSizeBuffer, DNSQueryResult]).arrayBuffer());
                    vlessHeaderIsSent = true;
                }
            }
        }
    }))
    .catch((error) => {
        console.log(`DNS UDP has error: ${error}`);
    });

    const writer = transfrom.writable.getWriter();
    return {
        /**
         * 
         * @param {Uint8Array} chunk 
         */
        UDPStreamWrite(chunk) {
            writer.write(chunk);
        }
    }
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