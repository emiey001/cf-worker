/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run "npm run dev" in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run "npm run deploy" to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */


export default {
    async fetch(request, env, ctx) {
      const url = new URL(request.url);
      const address = url.searchParams.get("address");
      if (!address) {
        return new Response("Address parameter is missing.", { status: 400 });
      }
      const response = await fetch(`http://${address}/cdn-cgi/trace`);
      return new Response(response.body, { status: 200 });
    },
};