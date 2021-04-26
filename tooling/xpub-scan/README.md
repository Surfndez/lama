# Automatic Xpub Scan Analysis (Prototype)

Given a Bitcoin xpub, this prototype automatically fetches the balance and operations history from LaMa and checks them using [Xpub Scan](https://github.com/LedgerHQ/xpub-scan).

## Configure

It is strongly advised to use a custom provider instead of the default one. To this end, rename `.env.template` into `.env` and provide the custom provider URL as well as your API key. For more information, please refer to Xpub Scan documentation.

## Build

```
$ docker build -t xpub-scan-lama .
```

## Run

### Option 1. LaMa is running on localhost

From `./tooling/xpub-scan` subdirectory:

```
$ docker run -v $(pwd)/report:/xpub-scan/report xpub-scan-lama --xpub <xpub> --scheme <scheme> 
```

Example:

```
$ docker run -v $(pwd)/report:/xpub-scan/report scan-the-lama --xpub xpub6DEH...vn7C2 --scheme bip84
```

### Option 2. Custom base url

From `./tooling/xpub-scan` subdirectory:

```
$ docker run -v $(pwd)/report:/xpub-scan/report scan-the-lama --xpub xpub6DEH...vn7C2 --scheme bip84 --base-url <base url:port>
```

Example:

```
$ docker run -v $(pwd)/report:/xpub-scan/report scan-the-lama --xpub xpub6DEH...vn7C2 --scheme bip84 --base-url https://lama-123.test.com:8080
```

## Test Execution Results

If the balance and operations from LaMa match with the expected one, a zero exit code is returned.

Otherwise, a non-zero exit code is returned as well as the diff between the balances and/or the operations.

## TODO

- Port to TypeScript
