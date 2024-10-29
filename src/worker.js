/**
 * This Cloudflare Worker handles incoming requests and manages VPN 
 * tunneling using the VLESS protocol. It processes query parameters 
 * to establish connections securely.
 * 
 * Original idea: https://github.com/zizifn/edgetunnel
 * Modified by: Ahmad Sysdev
 */


import { connect } from 'cloudflare:sockets';


// Global variables
const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;
let userID = '63fc3853-3fd5-444c-8dca-d8579bec588e'; // python -c 'import uuid;print(uuid.uuid4())'
let proxy = '';

export default {
    /**
     * 
     * @param {import("@cloudflare/workers-types").Request} request 
     * @param {{ UUID: string, proxy: string}} env Your environment variables 
     * @param {import("@cloudflare/workers-types").ExecutionContext} context 
     */
    async fetch(request, env, context) {
        userID = env.UUID || userID;
        proxy = env.proxy || proxy;
        try {
            const upgradeHeader = request.headers.get('Upgrade');
            const url = new URL(request.url);
            if (!upgradeHeader || upgradeHeader !== 'websocket') {
                switch (url.pathname) {
                    case '/':
                        return new Response('Hello world!', { status: 200 });
                    default:
                        return new Response('Not found.', { status: 404 });
                }
            }
            else {
                return await WSHandler(request, env, context)
            }
        } catch (error) {
            return new Response(e.toString(), { status: 503 });
        }
    }
}

/**
 * 
 * @param {import("@cloudflare/workers-types").Request} request 
 * @param {Object} env 
 * @param {import("@cloudflare/workers-types").ExecutionContext} context 
 */
async function WSHandler(request, env, context) {
    /**
     * @type {import("@cloudflare/workers-types").WebSocket[]}
     */
    const [client, webSocket] = new WebSocketPair();
    webSocket.accept();

    let rstreamCancel = false;
    const rstream = new ReadableStream({
        start(controller) {
            webSocket.addEventListener('message', (event) => {
                if (rstreamCancel) {
                    return;
                }
                controller.enqueue(event.data);
            });

            webSocket.addEventListener('close', () => {
                if (webSocket.readyState === WS_READY_STATE_OPEN || webSocket.readyState === WS_READY_STATE_CLOSING) {
                    webSocket.close();
                }
                if (rstreamCancel) {
                    return;
                }
                controller.close();
            });

            webSocket.addEventListener('error', (error) => {
                // error(error.stack)
                controller.error(error);
            });

            // For WS 0-RTT
            const earlyDataHeader = request.headers.get('sec-websocket-protocol');
            if (earlyDataHeader) {
                try {
                    const decode = atob(earlyDataHeader.replace(/-/g, '+').replace(/_/g, '/'))
                    const buff = Uint8Array.from(decode, (c) => c.charCodeAt(0));
                    controller.enqueue(buff.buffer);
                }
                catch (error) {
                    // error(error.stack)
                    controller.error(error);
                }
            }
        },
        pull(controller) {
            // 
        },
        cancel(reason) {
            if (rstreamCancel) {
                return;
            }
            rstreamCancel = true;
            if (webSocket.readyState === WS_READY_STATE_OPEN || webSocket.readyState === WS_READY_STATE_CLOSING) {
                webSocket.close();
            }
        }
    });

    /**
     * @type {{ value: import("@cloudflare/workers-types").Socket }}
     */
    let remoteSocketWrapper = {
        value: null,
    };
    let UDPStreamWrite = null;
    let isDNS = false;

    // WebSocket --> remote
    rstream.pipeTo(new WritableStream({
        async write(chunk, controller) {
            if (isDNS && UDPStreamWrite) {
                UDPStreamWrite.write(chunk);
                UDPStreamWrite.releaseLock();
                return;
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
                port = 443,
                address = '',
                rawDataIndex,
                vlessVersion = new Uint8Array([0, 0]),
                isUDP
            } = vlessHeaderParser(chunk);
            if (hasError) {
                throw new Error(message);
            }
            if (isUDP) {
                if (port === 53) {
                    isDNS = true;
                }
                else {
                    throw new Error(`UDP isn't supported.`);
                }
            }
            const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
            const rawClientData = chunk.slice(rawDataIndex);

            if (isDNS) {
                UDPStreamWrite = await handleUDPOutBound(webSocket, vlessResponseHeader);
                UDPStreamWrite.write(rawClientData);
                UDPStreamWrite.releaseLock();
                return;
            }
            handleTCPOutBound(remoteSocketWrapper, address, port, rawClientData, webSocket, vlessResponseHeader);
        },
        close() {
            // info('ReadableWebSocketStream was closed.');
        },
        abort(reason) {
            // warn(`ReadableWebSocketStream was aborted, due to ${reason}`);
        }
    }))
    .catch((error) => {
        // error(error.stack);
        // error(`ReadableWebSocketStream pipeTo error: ${error.stack}`);
    });
    return new Response(null, {
        status: 101,
        webSocket: client,
    });
}

/**
 * @param {ArrayBuffer} buffer
 * @returns {Object}
 */
function vlessHeaderParser(buffer) {
    if (buffer.byteLength < 24) {
        return {
            hasError: true,
            message: 'Invalid data.',
        };
    }
    const version = new Uint8Array(buffer.slice(0, 1));
    let isUDP = false;
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
        isUDP = true;
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
    const port = new DataView(portBuffer).getUint16(0);

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
        address: addressValue,
        addressType,
        port,
        rawDataIndex: addressValueIndex + addressLength,
        vlessVersion: version,
        isUDP,
    };
}

/**
 * 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocket 
 * @param {ArrayBuffer} vlessResponseHeader 
 * 
 */
async function handleUDPOutBound(webSocket, vlessResponseHeader) {
    let vlessHeaderIsSent = false;
    const transform = new TransformStream({
        start(controller) {
            // 
        },
        transform(chunk, controller) {
            for (let index = 0; index < chunk.byteLength;) {
                const lengthBuffer = chunk.slice(index, index + 2);
                const UDPPacketLength = new DataView(lengthBuffer).getUint16(0);
                const UDPData = new Uint8Array(chunk.slice(index + 2, index + 2 + UDPPacketLength));
                index = index + 2 + UDPPacketLength;
                controller.enqueue(UDPData);
            }
        },
        flush(controller) {
            // 
        }
    });
    transform.readable.pipeTo(new WritableStream({
        async write(chunk) {
            const response = await fetch('https://1.1.1.1/dns-query',
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/dns-message',
                    },
                    body: chunk,
                }
            );
            const DNSQueryResult = await response.arrayBuffer();
            const UDPSize = DNSQueryResult.byteLength;
            const UDPSizeBuffer = new Uint8Array([(UDPSize >> 8) & 0xff, UDPSize & 0xff]);
            if (webSocket.readyState === WS_READY_STATE_OPEN) {
                if (vlessHeaderIsSent) {
                    webSocket.send(await new Blob([UDPSizeBuffer, DNSQueryResult]).arrayBuffer());
                }
                else {
                    webSocket.send(await new Blob([vlessResponseHeader, UDPSizeBuffer, DNSQueryResult]).arrayBuffer());
                    vlessHeaderIsSent = true;
                }
            }
        }
    }))
    .catch((error) => {
        // error(`DNS UDP error: ${error}`);
    });

    const writer = transform.writable.getWriter();
    return writer
}

/**
 * 
 * @param {{ value: import("@cloudflare/workers-types").Socket }} remoteSocketWrapper
 * @param {string} address Ex: www.google.com
 * @param {number} port 
 * @param {Uint8Array} rawClientData 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocket 
 * @param {Uint8Array} vlessResponseHeader 
 * @returns {Promise<void>}
 */
async function handleTCPOutBound(remoteSocketWrapper, address, port, rawClientData, webSocket, vlessResponseHeader) {
    /**
     * 
     * @param {string} address 
     * @param {number} port 
     */
    async function connectAndWrite(address, port) {
        /**
         * @type {import("@cloudflare/workers-types").Socket}
         */
        const TCPSocket = connect({
            hostname: address,
            port
        });
        remoteSocketWrapper.value = TCPSocket;
        // // info(`Connected to ${address}:${port}`);
        const writer = TCPSocket.writable.getWriter();
        await writer.write(rawClientData);
        writer.releaseLock();
        return TCPSocket;
    }

    async function retry() {
        const TCPSocket = await connectAndWrite(proxy || address, port);
        TCPSocket.closed.catch((error) => {
            // error(`Retry TCPSocket closed error ${error.stack}`);
        })
        .finally(() => {
            if (webSocket.readyState === WS_READY_STATE_CLOSING || webSocket.readyState === WS_READY_STATE_OPEN) {
                webSocket.close();
            }
        });
        remoteSocketToWS(TCPSocket, webSocket, vlessResponseHeader);
    }

    const TCPSocket = await connectAndWrite(address, port);
    remoteSocketToWS(TCPSocket, webSocket, vlessResponseHeader, retry)
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
 * 
 * @param {import("@cloudflare/workers-types").Socket} TCPSocket 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocket 
 * @param {Uint8Array} vlessResponseHeader 
 * @param {function} retry 
 */
async function remoteSocketToWS(TCPSocket, webSocket, vlessResponseHeader, retry = null) {
    // Remote --> WebSocket
    let hasIncomingData = false;
    await TCPSocket.readable.pipeTo(new WritableStream({
        
        start() {
            // 
        },
        async write(chunk, controller) {
            hasIncomingData = true;
            if (webSocket.readyState !== WS_READY_STATE_OPEN) {
                // error(`webSocket.readyState is closed.`);
                controller.error(`webSocket.readyState is closed.`);
            }
            if (vlessResponseHeader) {
                webSocket.send(await new Blob([vlessResponseHeader, chunk]).arrayBuffer());
                vlessResponseHeader = null;
            }
            else {
                webSocket.send(chunk);
            }
        },
        close() {
            // warn(`TCPSocket.readable is close with incoming data is ${hasIncomingData}`);
        },
        abort(reason) {
            // error(`TCPSocket connection aborted due to ${reason}`);
        }
    }))
    .catch((error) => {
        // error(`TCPSocket have exception: ${error.stack || error}`);
        if (webSocket.readyState === WS_READY_STATE_CLOSING || webSocket.readyState === WS_READY_STATE_OPEN) {
            webSocket.close();
        }
    })
    if (hasIncomingData === false && retry) {
        // info('Retry');
        retry();
    }
}