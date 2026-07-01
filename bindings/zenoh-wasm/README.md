# zenoh-wasm

Browser WebAssembly bindings for Zenoh over WebSocket transports.

```js
import init, { initPanicHook, open } from "zenoh-wasm";

await init();
initPanicHook();

const session = await open("ws/127.0.0.1:7447");
await session.putString("demo/hello", "hello from wasm");
await session.putBytes("demo/flatbuffer", new Uint8Array([1, 2, 3, 4]));
await session.close();
```

Run a Zenoh router with a WebSocket listener:

```sh
zenohd -l ws/0.0.0.0:7447
```

For custom Zenoh configuration, pass JSON5 to `openWithConfig()`:

```js
import init, { openWithConfig } from "zenoh-wasm";

await init();

const session = await openWithConfig(`{
  mode: "client",
  connect: {
    endpoints: ["ws/127.0.0.1:7447"]
  }
}`);
```

This package is browser-oriented and uses `wasm32-unknown-unknown`. Use async APIs only. Blocking `.wait()` APIs, multicast scouting, listeners, dynamic plugins, and low-latency transport are not supported in the browser wasm build.

## Releasing

Build package artifacts locally:

```sh
NPM_PACKAGE_NAME='@cognipilot/zenoh-wasm' \
NPM_PACKAGE_VERSION='1.9.0-wasm.0' \
NPM_REPOSITORY_URL='git+https://github.com/CogniPilot/zenoh.git' \
node bindings/zenoh-wasm/scripts/build-package.mjs
```

Then publish from the generated package directory:

```sh
npm publish bindings/zenoh-wasm/pkg --tag next --access public
```

The GitHub Actions workflow `Publish zenoh-wasm npm` uses the same builder. Set `NPM_TOKEN` as a repository secret. The package name can come from the workflow input, `vars.NPM_PACKAGE_NAME`, or the neutral default `zenoh-wasm`.
