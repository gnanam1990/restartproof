/* Minimal blocking HTTP/1.0 client for worker -> receipt sink calls.
 * url form: http://host:port/path. Response is capped at cap bytes. */
#ifndef RP_HTTP_H
#define RP_HTTP_H

int rp_http_post(const char *url, const char *bearer, const char *body,
                 int timeout_ms, int *status, char *resp, int cap);

#endif
