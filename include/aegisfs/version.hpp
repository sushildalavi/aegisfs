#pragma once

namespace aegisfs {

// Semantic version of the AegisFS build. Bumped on releases.
inline constexpr const char* kVersion = "0.1.0";

// Build-time protocol version is in common.hpp (kProtocolVersion).
inline constexpr const char* kBuildName = "AegisFS";

}  // namespace aegisfs
