import time
import urllib
import hmac
import hashlib
from requests.auth import AuthBase


class BitmexAuth(AuthBase):
    """Attaches API Key Authentication to the given Request object. This implementation uses `expires`."""

    def __init__(self, api_key, api_secret):
        """Init with Key & Secret."""
        self.api_key = api_key
        self.api_secret = api_secret

    def __call__(self, r):
        """
        Called when forming a request - generates api key headers. This call uses `expires` instead of nonce.

        This way it will not collide with other processes using the same API Key if requests arrive out of order.
        For more details, see https://www.bitmex.com/app/apiKeys
        """
        # modify and return the request
        expires = int(round(time.time()) + 5)  # 5s grace period in case of clock skew
        r.headers["api-expires"] = str(expires)
        r.headers["api-key"] = self.api_key
        r.headers["api-signature"] = self.__generate_signature(
            self.api_secret, r.method, r.url, expires, r.body or ""
        )

        return r

    def __generate_signature(self, secret, verb, url, nonce, data):
        """Generate a request signature compatible with BitMEX."""
        # Parse the url so we can remove the base and extract just the path.
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        if parsed_url.query:
            path = path + "?" + parsed_url.query

        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf8")

        message = verb + path + str(nonce) + data
        signature = hmac.new(
            bytes(self.api_secret, "utf8"),
            bytes(message, "utf8"),
            digestmod=hashlib.sha256,
        ).hexdigest()
        return signature
