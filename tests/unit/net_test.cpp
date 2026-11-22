#include <gtest/gtest.h>

#include <thread>

#include "aegisfs/net.hpp"

using namespace aegisfs;

TEST(NetTest, ParseEndpoint) {
  std::string h; int p;
  ASSERT_TRUE(parse_endpoint("127.0.0.1:9100", h, p));
  EXPECT_EQ(h, "127.0.0.1");
  EXPECT_EQ(p, 9100);
  EXPECT_TRUE(parse_endpoint("host.example:1", h, p));
  EXPECT_FALSE(parse_endpoint("noport", h, p));
  EXPECT_FALSE(parse_endpoint(":9100", h, p));
  EXPECT_FALSE(parse_endpoint("host:", h, p));
  EXPECT_FALSE(parse_endpoint("host:70000", h, p));  // out of range
  EXPECT_FALSE(parse_endpoint("host:abc", h, p));
}

TEST(NetTest, FrameRoundTripOverSocket) {
  Listener l = Listener::bind("127.0.0.1", 0);
  ASSERT_TRUE(l.valid());
  int port = l.port();

  std::string got_payload;
  Fields got_header;
  MsgType got_type{MsgType::Reply};
  std::thread server([&] {
    Conn c = l.accept();
    ASSERT_TRUE(c.valid());
    Frame fr;
    ASSERT_EQ(read_frame(c, fr), Status::Ok);
    got_type = fr.type;
    got_header = fr.header;
    got_payload = fr.payload;
    // echo a reply
    Fields rh;
    rh.set_i64("status", static_cast<int64_t>(Status::Ok));
    write_frame(c, MsgType::Reply, rh, "pong");
  });

  Conn c = Conn::dial("127.0.0.1", port, 2000);
  ASSERT_TRUE(c.valid());
  Fields h;
  h.set("file", "a/b");
  h.set_u64("n", 42);
  std::string payload(5000, 'Z');
  ASSERT_EQ(write_frame(c, MsgType::Create, h, payload), Status::Ok);
  Frame reply;
  ASSERT_EQ(read_frame(c, reply), Status::Ok);
  EXPECT_EQ(reply.payload, "pong");

  server.join();
  EXPECT_EQ(got_type, MsgType::Create);
  EXPECT_EQ(got_header.get_or("file"), "a/b");
  EXPECT_EQ(got_header.get_u64("n"), 42u);
  EXPECT_EQ(got_payload, payload);
}

TEST(NetTest, RejectsBadMagicFrame) {
  Listener l = Listener::bind("127.0.0.1", 0);
  ASSERT_TRUE(l.valid());
  int port = l.port();
  Status server_status = Status::Ok;
  std::thread server([&] {
    Conn c = l.accept();
    MsgType t; Fields h; uint64_t plen = 0;
    server_status = read_message_head(c, t, h, plen);  // must reject cleanly
  });
  Conn c = Conn::dial("127.0.0.1", port, 2000);
  ASSERT_TRUE(c.valid());
  // 12-byte head with a wrong magic word
  unsigned char bad[12] = {0xDE, 0xAD, 0xBE, 0xEF, 1, 0, 1, 0, 0, 0, 0, 0};
  c.write_all(bad, sizeof(bad));
  server.join();
  EXPECT_EQ(server_status, Status::InvalidArgument);
}

TEST(NetTest, StreamingRoundTrip) {
  Listener l = Listener::bind("127.0.0.1", 0);
  ASSERT_TRUE(l.valid());
  int port = l.port();
  const size_t N = 3 * 1024 * 1024 + 123;  // spans multiple 1 MiB chunks

  std::string received;
  std::thread server([&] {
    Conn c = l.accept();
    MsgType t; Fields h; uint64_t plen = 0;
    ASSERT_EQ(read_frame_streaming(c, t, h, plen,
                                   [&](const char* b, size_t n) { received.append(b, n); return true; }),
              Status::Ok);
    EXPECT_EQ(plen, N);
  });

  Conn c = Conn::dial("127.0.0.1", port, 2000);
  ASSERT_TRUE(c.valid());
  Fields h;
  size_t sent = 0;
  Status s = write_frame_streaming(c, MsgType::ReplicatePut, h, N, [&](char* b, size_t cap) -> size_t {
    size_t n = std::min(cap, N - sent);
    for (size_t i = 0; i < n; ++i) b[i] = static_cast<char>((sent + i) & 0xFF);
    sent += n;
    return n;
  });
  ASSERT_EQ(s, Status::Ok);
  server.join();
  ASSERT_EQ(received.size(), N);
  for (size_t i = 0; i < N; ++i) ASSERT_EQ(static_cast<unsigned char>(received[i]), i & 0xFF);
}
