#pragma once

namespace aegisfs {

// Parse argv and dispatch to `serve` (run a node) or a client command. Returns a process exit code.
int cli_main(int argc, char** argv);

}  // namespace aegisfs
