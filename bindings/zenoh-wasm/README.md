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

## Releasing From CI

The `Release zenoh-wasm npm` workflow publishes npm packages only after its release checks pass for a tag that matches `zenoh-wasm-npm-v<version>`.

Configure these repository settings before publishing:

- Secret `NPM_TOKEN`: npm automation token with publish access.
- Variable `NPM_PACKAGE_NAME`: npm package name, for example `@cognipilot/zenoh-wasm`.
- Variable `NPM_TAG`: npm dist-tag, for example `next`.
- Variable `NPM_RELEASE_BRANCH`: release branch name. Defaults to `cognipilot/zenoh-wasm-npm`.

Create a release tag on the release branch:

```sh
git switch cognipilot/zenoh-wasm-npm
git tag -a zenoh-wasm-npm-v1.9.0-wasm.0 -m 'zenoh wasm npm 1.9.0-wasm.0'
git push cognipilot cognipilot/zenoh-wasm-npm
git push cognipilot zenoh-wasm-npm-v1.9.0-wasm.0
```

The version in the tag becomes the npm package version. The builder also passes that version into the wasm module, so `version()` reports the published npm version.

Build package artifacts locally without publishing:

```sh
NPM_PACKAGE_NAME='@cognipilot/zenoh-wasm' \
NPM_PACKAGE_VERSION='1.9.0-wasm.0' \
NPM_REPOSITORY_URL='git+https://github.com/CogniPilot/zenoh.git' \
node bindings/zenoh-wasm/scripts/build-package.mjs
```

To publish manually from the generated package directory:

```sh
npm publish bindings/zenoh-wasm/pkg --tag next --access public --provenance
```
