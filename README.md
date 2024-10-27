# Cloudflare Worker for VLESS Protocol VPN Tunneling

This Cloudflare Worker enables secure VPN tunneling using the VLESS protocol. It processes incoming requests and handles WebSocket connections for data transfer.

## Features

- Establishes secure WebSocket connections.
- Supports VLESS protocol for VPN tunneling.
- Handles both TCP and DNS (UDP) requests.
- Easy integration with Cloudflare's serverless architecture.

## Prerequisites

- A Cloudflare account.
- Knowledge of the VLESS protocol.
- Basic understanding of WebSocket connections.

## Environment Variables

Make sure to set the following environment variables when deploying:

- `UUID`: A unique identifier for the user, preferably generated using Python's `uuid` library.
- `proxy`: Optional proxy address for TCP connections.

## Setup

You can set up the Worker using one of the following methods:

### Method 1: Using Wrangler CLI

1. **Clone the Repository**

Clone this repository to your local machine:

```bash
git clone https://github.com/ahmadsysdev/cf-worker
```

2. **Edit the Wrangler Configuration**
Navigate to the cloned directory:

```bash
cd cf-worker
```

Open the `wrangler.toml` file and configure it by setting your worker's `name`, `UUID` and adding your environment variables:

```toml
name = "your worker's name here"
main = "src/worker.js"

[vars]
UUID = '0727bd9a-15a3-4928-87ad-fae38f153499'
```

3. **Login to Wrangler**

Log in to your Cloudflare account using Wrangler:

```bash
wrangler login
```

4. **Deploy your Worker to Cloudflare:**

```bash
wrangler publish
```

### Method 2: Directly in the Cloudflare Dashboard

1. **Copy the Worker Code**

Open the `src/worker.js` file in your favorite text editor, copy all the code to your clipboard.

2. **Go to the Cloudflare Dashboard**

Log in to your Cloudflare account and navigate to the "Workers" section.

3. **Create a New Worker**

Click on "Create" or "Add a Worker."

4. **Paste the Code**

In the Worker editor, delete any existing code and paste the copied code from `worker.js`.

5. **Set Environment Variables**

In the "Settings" for your Worker, add the required environment variables:

- `UUID`: A unique identifier for the user.
- `proxy`: Optional proxy address for TCP connections.

6. **Save and Deploy**

Click "Save" and then "Deploy" to publish your Worker.

## Usage

### WebSocket Connection

- The Worker listens for incoming WebSocket requests. When a valid request is made, it establishes a WebSocket connection.

### TCP and DNS Requests

- Upon receiving data, the Worker parses the VLESS header to determine if it's a TCP or DNS request and routes it accordingly.

### Error Handling

The Worker implements basic error handling:
- Invalid user IDs or protocol types return appropriate error messages.
- WebSocket errors will close the connection gracefully.

## Features

- **Establishes secure WebSocket connections.**
- **Supports VLESS protocol for VPN tunneling.**
- **Currently supports DNS (UDP) requests.**
- **TCP requests and additional protocols (like Trojan and VMess) may be supported in the future.**

## Contribution

Feel free to modify or extend the Worker as needed. Contributions to improve functionality or enhance security are welcome!

## License

This project is open source. Feel free to use, modify, and distribute it under your own terms.

## Acknowledgements

- Inspired by [zizifn/edgetunnel](https://github.com/zizifn/edgetunnel)
- Modified by [Ahmad Sysdev](https://github.com/ahmadsysdev)