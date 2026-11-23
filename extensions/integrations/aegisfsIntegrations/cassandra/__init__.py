"""Optional Cassandra metadata index for AegisFS.

The index records what AegisFS reported about each file (name, version, size when known, replica
locations, a checksum when one was actually computed) so names can be listed and looked up without
scanning storage nodes. It is an observation, never the authority: AegisFS can be used with this
package absent, and an index entry never overrides what AegisFS says.
"""
from .hook import observe  # noqa: F401
