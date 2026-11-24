FROM quay.io/keycloak/keycloak:26.2.5 AS builder

ENV KC_DB=postgres
ENV KC_HEALTH_ENABLED=true
ENV KC_METRICS_ENABLED=true

RUN /opt/keycloak/bin/kc.sh build

FROM quay.io/keycloak/keycloak:26.2.5

COPY --from=builder /opt/keycloak/ /opt/keycloak/
COPY entrypoint.sh /opt/keycloak/bin/entrypoint.sh

ENTRYPOINT ["/opt/keycloak/bin/entrypoint.sh"]
