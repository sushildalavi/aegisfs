#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include "aegisfs/net.hpp"
#include "support/clusterFixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;
namespace fs = std::filesystem;

namespace {

// Restores the process-wide transport settings no matter how a test ends.
struct TransportReset {
  ~TransportReset() {
    configureTls(TlsConfig{});
    configureAuthToken("");
  }
};

Status rawStatus(const std::string& addr, MsgType type, const Fields& header) {
  std::string host; int port = 0;
  if (!parseEndpoint(addr, host, port)) return Status::InvalidArgument;
  Conn c = Conn::dial(host, port, 3000);
  if (!c.valid()) return Status::NetworkError;
  if (!isOk(writeFrame(c, type, header, ""))) return Status::NetworkError;
  Frame reply;
  if (!isOk(readFrame(c, reply))) return Status::NetworkError;
  return static_cast<Status>(reply.header.getI64("status", static_cast<int64_t>(Status::Internal)));
}

// Builds a CA and a node certificate for 127.0.0.1 under a fresh temp directory.
struct Pki {
  std::string dir, ca, cert, key;
  bool ok{false};
  explicit Pki(const std::string& tag) {
    dir = (fs::temp_directory_path() / ("aegisfs_pki_" + tag + "_" + std::to_string(::getpid()))).string();
    fs::remove_all(dir);
    fs::create_directories(dir);
    std::string cd = "cd '" + dir + "' && ";
    std::string quiet = " >/dev/null 2>&1";
    int rc = std::system((cd + "openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=test-ca "
                          "-keyout ca.key -out ca.pem" + quiet).c_str());
    rc |= std::system((cd + "openssl req -newkey rsa:2048 -nodes -subj /CN=node -keyout node.key "
                       "-out node.csr" + quiet).c_str());
    rc |= std::system((cd + "printf 'subjectAltName=IP:127.0.0.1\\nextendedKeyUsage=serverAuth,clientAuth\\n' "
                       "> ext && openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key "
                       "-CAcreateserial -days 2 -extfile ext -out node.pem" + quiet).c_str());
    ca = dir + "/ca.pem"; cert = dir + "/node.pem"; key = dir + "/node.key";
    ok = rc == 0;
  }
  ~Pki() { fs::remove_all(dir); }
};

TlsConfig tlsFor(const Pki& p, bool mutual) {
  TlsConfig t;
  t.enabled = true;
  t.certificate = p.cert;
  t.privateKey = p.key;
  t.caFile = p.ca;
  t.clientCertificate = p.cert;
  t.clientPrivateKey = p.key;
  t.requireClientCertificate = mutual;
  return t;
}

}  // namespace

TEST(Security, TokenIsRequiredOnEveryRequest) {
  TransportReset reset;
  configureAuthToken("correct-horse");
  Cluster cl(3);
  ASSERT_TRUE(cl.awaitConvergence(3));

  // Clients that carry the token work, and so do the nodes' own gossip and replication.
  std::string in = makeTempFile("tok", 5000);
  Client c = cl.client(0, "c");
  ASSERT_TRUE(c.create(in, "secure/file").ok());

  // Nodes in this test share the process-wide token with the client, so the "no token" case is
  // forged by sending a Reply-typed frame, which the framing layer never stamps with a token.
  // The node must refuse it before looking at what it asks for. Wrong tokens are covered by
  // AuthTokenComparison, and the real-process case by scripts/checks/reliabilityTest.sh.
  EXPECT_EQ(rawStatus(cl.addr(1), MsgType::Reply, Fields{}), Status::Unauthorized);
  EXPECT_EQ(rawStatus(cl.addr(1), MsgType::Health, Fields{}), Status::Ok);
}

TEST(Security, AuthTokenComparison) {
  TransportReset reset;
  EXPECT_TRUE(authTokenMatches("anything"));  // no token configured, no check
  configureAuthToken("abc");
  EXPECT_TRUE(authTokenMatches("abc"));
  EXPECT_FALSE(authTokenMatches(""));
  EXPECT_FALSE(authTokenMatches("abd"));
  EXPECT_FALSE(authTokenMatches("abcd"));
  EXPECT_FALSE(authTokenMatches("ab"));
}

TEST(Security, TlsClusterWorksAndRejectsOutsiders) {
  TransportReset reset;
  Pki pki("tls");
  if (!pki.ok) GTEST_SKIP() << "openssl command line not available";
  configureTls(tlsFor(pki, /*mutual=*/false));
  Cluster cl(3);
  ASSERT_TRUE(cl.awaitConvergence(3));

  std::string in = makeTempFile("tls", 300000);
  Client c = cl.client(0, "c");
  ASSERT_TRUE(c.create(in, "tls/file").ok());
  Client reader = cl.client(2, "r");
  std::string out = in + ".out";
  ASSERT_TRUE(reader.get("tls/file", out).ok());
  EXPECT_EQ(fileShaHex(in), fileShaHex(out));

  // A plaintext client cannot talk to a TLS cluster.
  configureTls(TlsConfig{});
  Client plain = cl.client(1, "p");
  EXPECT_FALSE(plain.health().ok());

  // A client that trusts a different CA refuses the cluster's certificate.
  Pki other("other");
  ASSERT_TRUE(other.ok);
  TlsConfig wrongCa;
  wrongCa.enabled = true;
  wrongCa.caFile = other.ca;
  configureTls(wrongCa);
  Client mistrust = cl.client(1, "m");
  EXPECT_FALSE(mistrust.health().ok());
}

TEST(Security, MutualTlsRejectsClientsWithoutACertificate) {
  TransportReset reset;
  Pki pki("mtls");
  if (!pki.ok) GTEST_SKIP() << "openssl command line not available";
  configureTls(tlsFor(pki, /*mutual=*/true));
  Cluster cl(3);
  ASSERT_TRUE(cl.awaitConvergence(3));
  EXPECT_TRUE(cl.client(0, "ok").health().ok());

  TlsConfig noCert;
  noCert.enabled = true;
  noCert.caFile = pki.ca;
  configureTls(noCert);
  EXPECT_FALSE(cl.client(0, "nocert").health().ok());
}
