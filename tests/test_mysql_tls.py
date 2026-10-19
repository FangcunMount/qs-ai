import datetime
import ipaddress
import socket
import ssl
import threading
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from qs_ai.infrastructure.persistence.mysql.tls import tls_context


def certificate(tmp_path: Path, *, expired=False, wrong_name=False):
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.UTC)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "MySQL TLS test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(hours=1))
        .not_valid_after(now + datetime.timedelta(minutes=-1 if expired else 60))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address("127.0.0.2" if wrong_name else "127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    ca = tmp_path / "ca.pem"
    ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    private = tmp_path / "key.pem"
    private.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return ca, private


@pytest.mark.parametrize("case", ["valid", "expired", "wrong_name", "wrong_ca", "no_tls"])
def test_verified_context_accepts_only_the_expected_peer_on_reconnect(tmp_path, case):
    ca, private = certificate(tmp_path, expired=case == "expired", wrong_name=case == "wrong_name")
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.minimum_version = ssl.TLSVersion.TLSv1_2
    server.load_cert_chain(ca, private)
    trusted = ca
    if case == "wrong_ca":
        other = tmp_path / "other"
        other.mkdir()
        trusted, _ = certificate(other)
    client = tls_context("VERIFY_IDENTITY", str(trusted))
    assert client is not None
    assert client.check_hostname and client.verify_mode == ssl.CERT_REQUIRED
    assert client.minimum_version == ssl.TLSVersion.TLSv1_2
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()

        def peer():
            for _ in range(2):
                connection, _ = listener.accept()
                with connection:
                    connection.settimeout(3)
                    if case == "no_tls":
                        connection.sendall(b"plaintext is not TLS")
                        continue
                    try:
                        with server.wrap_socket(connection, server_side=True) as secure:
                            secure.recv(1)
                    except ssl.SSLError:
                        pass

        thread = threading.Thread(target=peer, daemon=True)
        thread.start()
        for _ in range(2):
            with socket.create_connection(listener.getsockname(), timeout=3) as connection:
                if case == "valid":
                    with client.wrap_socket(connection, server_hostname="127.0.0.1") as secure:
                        assert secure.cipher()
                        secure.sendall(b"x")
                else:
                    with pytest.raises(ssl.SSLError):
                        client.wrap_socket(connection, server_hostname="127.0.0.1")
        thread.join(timeout=5)
        assert not thread.is_alive()


def test_missing_or_invalid_ca_and_mode_fail_before_connect(tmp_path):
    assert tls_context("DISABLED", None) is None
    with pytest.raises(ValueError):
        tls_context("PREFERRED", None)
    with pytest.raises(ValueError):
        tls_context("VERIFY_IDENTITY", None)
    with pytest.raises(FileNotFoundError):
        tls_context("VERIFY_IDENTITY", str(tmp_path / "missing"))
    invalid = tmp_path / "invalid"
    invalid.write_text("not a certificate")
    with pytest.raises(ssl.SSLError):
        tls_context("VERIFY_IDENTITY", str(invalid))
