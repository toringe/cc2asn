"""HTTP client for the CC2ASN web API, used by the whois bridge.

Send latest-day lookups on behalf of a whois client. The bridge secret plus the
originating client's IP travel in the Authorization and X-Forwarded-For headers;
the API trusts the forwarded IP only when the secret matches.

Request get(api_base, secret, client_ip, rectype, cc) ->
    (status, body) where status is the HTTP status code, or 0 when the API
    cannot be reached (connection error / timeout).
"""

import urllib.request

TIMEOUT = 5.0  # seconds


def get(api_base, secret, client_ip, rectype, cc, timeout=TIMEOUT):
    url = f"{api_base}/{rectype.upper()}/{cc.upper()}"
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {secret}",
            "X-Forwarded-For": client_ip,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as err:
        try:
            return err.code, err.read()
        finally:
            err.close()
    except OSError:
        # URLError (DNS/refused/timeout) and socket errors both subclass OSError
        return 0, b""
