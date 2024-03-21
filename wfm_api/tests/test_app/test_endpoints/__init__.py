"""Creates a mock container that will be used for testing enpoints.
"""

import os
import unittest
from pathlib import Path

from httpx import Response
import respx

from pax.entrypoint import create_container
from pax.providers.oidc.models import UserClaims

from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.pax_hooks.wfm_database import wfm_database_hook
from wfm_api.routers import wfm_routers
from wfm_api.utils.database.wfm_database import Session, Service, StepDescription
from wfm_api.utils.database.wfm_database import WFMDatabase

CURRENT_DIR = Path(__file__).parent.absolute()
TEST_CONFIG = CURRENT_DIR / "test_data" / "settings.yaml"
TEST_DATABASE = f"/tmp/{os.environ['USER']}-wfm-api_test.db"
TEST_WDF_SBB_OK = CURRENT_DIR / "test_data" / "wdf1_sbb.yaml"
TEST_WDF_NFS_OK = CURRENT_DIR / "test_data" / "wdf1_nfs.yaml"


# Mock response sent by issuer URL
MOCK_KEYCLOAK_RESPONSE = {"issuer": "http://localhost/auth/realms/bird", "authorization_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/auth", "token_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/token", "introspection_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/token/introspect", "userinfo_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/userinfo", "end_session_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/logout", "frontchannel_logout_session_supported": True, "frontchannel_logout_supported": True, "jwks_uri": "http://localhost/auth/realms/bird/protocol/openid-connect/certs", "check_session_iframe": "http://localhost/auth/realms/bird/protocol/openid-connect/login-status-iframe.html", "grant_types_supported": ["authorization_code", "implicit", "refresh_token", "password", "client_credentials", "urn:ietf:params:oauth:grant-type:device_code", "urn:openid:params:grant-type:ciba"], "response_types_supported": ["code", "none", "id_token", "token", "id_token token", "code id_token", "code token", "code id_token token"], "subject_types_supported": ["public", "pairwise"], "id_token_signing_alg_values_supported": ["PS384", "ES384", "RS384", "HS256", "HS512", "ES256", "RS256", "HS384", "ES512", "PS256", "PS512", "RS512"], "id_token_encryption_alg_values_supported": ["RSA-OAEP", "RSA-OAEP-256", "RSA1_5"], "id_token_encryption_enc_values_supported": ["A256GCM", "A192GCM", "A128GCM", "A128CBC-HS256", "A192CBC-HS384", "A256CBC-HS512"], "userinfo_signing_alg_values_supported": ["PS384", "ES384", "RS384", "HS256", "HS512", "ES256", "RS256", "HS384", "ES512", "PS256", "PS512", "RS512", "none"], "request_object_signing_alg_values_supported": ["PS384", "ES384", "RS384", "HS256", "HS512", "ES256", "RS256", "HS384", "ES512", "PS256", "PS512", "RS512", "none"], "request_object_encryption_alg_values_supported": ["RSA-OAEP", "RSA-OAEP-256", "RSA1_5"], "request_object_encryption_enc_values_supported": ["A256GCM", "A192GCM", "A128GCM", "A128CBC-HS256", "A192CBC-HS384", "A256CBC-HS512"], "response_modes_supported": ["query", "fragment", "form_post", "query.jwt", "fragment.jwt", "form_post.jwt", "jwt"], "registration_endpoint": "http://localhost/auth/realms/bird/clients-registrations/openid-connect", "token_endpoint_auth_methods_supported": ["private_key_jwt", "client_secret_basic", "client_secret_post", "tls_client_auth", "client_secret_jwt"], "token_endpoint_auth_signing_alg_values_supported": ["PS384", "ES384", "RS384", "HS256", "HS512", "ES256", "RS256", "HS384", "ES512", "PS256", "PS512", "RS512"], "introspection_endpoint_auth_methods_supported": ["private_key_jwt", "client_secret_basic", "client_secret_post", "tls_client_auth", "client_secret_jwt"], "introspection_endpoint_auth_signing_alg_values_supported": [
    "PS384", "ES384", "RS384", "HS256", "HS512", "ES256", "RS256", "HS384", "ES512", "PS256", "PS512", "RS512"], "authorization_signing_alg_values_supported": ["PS384", "ES384", "RS384", "HS256", "HS512", "ES256", "RS256", "HS384", "ES512", "PS256", "PS512", "RS512"], "authorization_encryption_alg_values_supported": ["RSA-OAEP", "RSA-OAEP-256", "RSA1_5"], "authorization_encryption_enc_values_supported": ["A256GCM", "A192GCM", "A128GCM", "A128CBC-HS256", "A192CBC-HS384", "A256CBC-HS512"], "claims_supported": ["aud", "sub", "iss", "auth_time", "name", "given_name", "family_name", "preferred_username", "email", "acr"], "claim_types_supported": ["normal"], "claims_parameter_supported": True, "scopes_supported": ["openid", "offline_access", "address", "microprofile-jwt", "roles", "web-origins", "profile", "email", "phone"], "request_parameter_supported": True, "request_uri_parameter_supported": True, "require_request_uri_registration": True, "code_challenge_methods_supported": ["plain", "S256"], "tls_client_certificate_bound_access_tokens": True, "revocation_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/revoke", "revocation_endpoint_auth_methods_supported": ["private_key_jwt", "client_secret_basic", "client_secret_post", "tls_client_auth", "client_secret_jwt"], "revocation_endpoint_auth_signing_alg_values_supported": ["PS384", "ES384", "RS384", "HS256", "HS512", "ES256", "RS256", "HS384", "ES512", "PS256", "PS512", "RS512"], "backchannel_logout_supported": True, "backchannel_logout_session_supported": True, "device_authorization_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/auth/device", "backchannel_token_delivery_modes_supported": ["poll", "ping"], "backchannel_authentication_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/ext/ciba/auth", "backchannel_authentication_request_signing_alg_values_supported": ["PS384", "ES384", "RS384", "ES256", "RS256", "ES512", "PS256", "PS512", "RS512"], "require_pushed_authorization_requests": False, "pushed_authorization_request_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/ext/par/request", "mtls_endpoint_aliases": {"token_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/token", "revocation_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/revoke", "introspection_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/token/introspect", "device_authorization_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/auth/device", "registration_endpoint": "http://localhost/auth/realms/bird/clients-registrations/openid-connect", "userinfo_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/userinfo", "pushed_authorization_request_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/ext/par/request", "backchannel_authentication_endpoint": "http://localhost/auth/realms/bird/protocol/openid-connect/ext/ciba/auth"}}
MOCK_CERTS = {"keys": [{
    "kty": "RSA",
    "e": "AQAB",
    "use": "sig",
    "alg": "RS256",
    "n": "tqlNNxQA0RoECI0jIQ4sm-pQdsFPi_kCAf9WbVMy59l5_CDRpCe6WtQTjhtDU7Qif0rLBH6Ad0HOMN3qELJfrKwYeaMhM45M7pIM499xa2AWz4m0WfOZRJq07F3QtzPaVSNuZAz2zLFsf0OLcVPOSLqt2UJGGGIiTVAKgcIQlZ0wq6zfpvnU0M6tM2U5ylTThs0k6JjELtGpKHvGX6rZYJ79QzLYZLcFfiO_IUrRuLRHWmIvalLsZjoeFDdvJcYggfGXqZwPGxK1rPctk-0LhZYe-6JNOvk6hTKo4jg4R-ke5BOVHuMfhzzkkLib0JguCeCoFbwsv1x69V7icuZbiw"
}]}
PRIVATE_KEY = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAtqlNNxQA0RoECI0jIQ4sm+pQdsFPi/kCAf9WbVMy59l5/CDR
pCe6WtQTjhtDU7Qif0rLBH6Ad0HOMN3qELJfrKwYeaMhM45M7pIM499xa2AWz4m0
WfOZRJq07F3QtzPaVSNuZAz2zLFsf0OLcVPOSLqt2UJGGGIiTVAKgcIQlZ0wq6zf
pvnU0M6tM2U5ylTThs0k6JjELtGpKHvGX6rZYJ79QzLYZLcFfiO/IUrRuLRHWmIv
alLsZjoeFDdvJcYggfGXqZwPGxK1rPctk+0LhZYe+6JNOvk6hTKo4jg4R+ke5BOV
HuMfhzzkkLib0JguCeCoFbwsv1x69V7icuZbiwIDAQABAoIBABx1pm01daceuZAn
hViqH7MvP9gw6FbztidfhDTGaMEM9l+dMWy1L+jk8CMNBmFtSI6ytYz+tL9gBmNA
wC5XzGZX7fxNuWeR/VSSduWuM7q6HvO5DduSA8hXnmbReyqALr1jJtj52B4uaqwt
dvVyTEqyo1GdbNRxvcxz8x8igHj+4C4mArTXzxtJc2iVyxkMOykeWujOisgyRJ5N
gyV1uNSPYu3Wrv2kYg4fkzl9Hv54teARmVz39191iCsc8LCbaljj2Qzdg6O6i4uJ
0PjeILZMOfkBABzdi9gmI/zfa3+ezXaF1TpjRO9trNv9pFbAwbDC6Z6J2sGIEg5q
jIRR9fECgYEA6yz51g0VGiCYe4wqoDeYBzWYUL0khInKVdlCpYY4dO/5bJclNxXQ
k7aR/nS7DCn97HviOZs+dbLFj39LogqAc99549TN2mnJIqGMvVHnDBNspVTZHPKg
PFNFTn3RntuSCutMxyDGtA1VbqaFsvFSFFl8eudRDOAMojWhHfEJ/GkCgYEAxtXq
pgxBJGkL1OO+yDv7y9tvWAOa3kaCepTXkNeeteno5MEsOJQWNlk9uyG8HDaLhmXf
EZkxsBpZXUA6gQcDHKtWfCF1Xdpfx8q5OU9oqarZXdeMtuRl6Ws2+7Lc3/8/qNw9
BUsxHtEPirttyw2HBeuLM9bCI3kNN+GY1vZ6qdMCgYEAxkpD1UFum3sEVpeWkUSO
wPVlmh4Anmf33G61jQ8gpyh3rCG81TdliEaVznDqDZWSbkT+OAg3n9G+VgHE6bnl
GM5C3eDeONydJMAGBNL79uih4L0r2waQKI0lkMrxZfpIp0BCmlt+bu4XLJbngDuN
M29IT/CeHDcFL/f8A1zrSJECgYBw7rkKvLUXIA7XSM9oXSFjpOu1ur5wdu3O/9D7
9GuxePyNSOZ78Cg5kDBOpBd6ksRmfl/XWAJvuld9bmiMNlZfJzXE5SALQWfbS1ou
odqZW2+ALFhA3LLBg+LDzNBE7W3T09tYsV9h2G4SZugyRymkhCcZN2Ymza9jSOAN
YAtFfwKBgBchCji3PtcT4xg7fqLsaBwFaIaB3pH+nmbF6OLlfVulVDMjyuzmhCUG
55PhwyMgd8crdFcVbVPoqX5u9QUv3rz76Qg6yYLKeQdb3X8VkDnLhZwn3u4hZkao
jcHBr4ETXN1XRKQzIp8UGyhcU9LfmLGRuP0HetWeP5XYhJYGMpT6
-----END RSA PRIVATE KEY-----"""

# mock user claims authentification
TEST_USER_CLAIMS = UserClaims(
    exp=0,
    iat=0,
    jti=0,
    iss="http://localhost/auth/realms/bird",
    typ="no-auth",
    azp="",
    session_state="",
    acr="",
    email_verified="",
    name="no-auth",
    preferred_username="no-auth",
    email="no-auth@example.com",
    # All roles used within the application should be listed here
    resource_access={"noauth-realm": {
        "roles": ["admin", "user", "wfm_admin", "wfm_user"]}
    },
    roles=["admin", "user", "wfm_admin", "wfm_user"]
)


class TestEntrypoints(unittest.TestCase):
    """Tests the entrypoints for creating containers work as expected."""

    @classmethod
    def setUpClass(cls):
        """Set up the tests by creating a mock Keycloak API.
           Creates test database.
        """
        # Mocked keycloak client
        cls.mocked_api = respx.mock(
            base_url="http://localhost/auth", assert_all_called=False)
        # Standard keycloak response
        well_known = cls.mocked_api.get("/realms/bird/.well-known/openid-configuration",
                                        name="well_known")
        well_known.return_value = Response(200, json=MOCK_KEYCLOAK_RESPONSE)
        public_key = cls.mocked_api.get("/realms/bird/protocol/openid-connect/certs",
                                        name="public_key")
        public_key.return_value = Response(200, json=MOCK_CERTS)
        cls.mocked_api.start()

    def setUp(self) -> None:
        """Setup the tests by creating container and initializing the WFM DB.
        """
        self.wdf_sbb_ok = TEST_WDF_SBB_OK
        self.wdf_nfs_ok = TEST_WDF_NFS_OK
        settings = WFMSettings.from_yaml(TEST_CONFIG)
        test_container = create_container(settings=settings,
                                          routers=wfm_routers,
                                          hooks=[wfm_database_hook])

        # Create testing app of WFM container
        self.test_app = test_container.test_client

        # remove test database if exists
        if os.path.exists(TEST_DATABASE):
            os.remove(TEST_DATABASE)

        # Create testing database
        test_wfm_db = WFMDatabase(TEST_DATABASE)

        # initialize database
        session1 = Session(name="s1", workflow_name="test1",
                           start_time=0, end_time=0, status="starting")
        service1 = Service(name="svc1", session_id=session1,
                           service_type="nfs", targets="/target1", flavor="flavor1",
                           datanodes=1, start_time=0, end_time=0)
        stepd1 = StepDescription(name="step1", session_id=session1, command="command1",
                                service_id=service1)
        session1.services = [service1]
        session1.step_descriptions = [stepd1]
        service1.step_descriptions = [stepd1]

        test_wfm_db.dbsession.add(session1)
        test_wfm_db.dbsession.add(service1)
        test_wfm_db.dbsession.add(stepd1)

        test_wfm_db.dbsession.commit()
        test_wfm_db.dbsession.close()

    def tearDown(self) -> None:
        # Remove the datadst file (see file TEST_WDF_NFS_OK)
        if os.path.isfile("/tmp/dataset.txt"):
            os.remove("/tmp/dataset.txt")
