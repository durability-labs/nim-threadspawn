--threads:
  on
--tlsEmulation:
  off
--styleCheck:
  usages
--styleCheck:
  error
--mm:orc
--define:"useMalloc" # needed to avoid TLSF bugs and cross thread mem alloc/free limitations

when (NimMajor, NimMinor) == (1, 2):
  switch("hint", "Processing:off")
  switch("hint", "XDeclaredButNotUsed:off")
  switch("warning", "ObservableStores:off")

when (NimMajor, NimMinor) > (1, 2):
  switch("hint", "XCannotRaiseY:off")
# begin Nimble config (version 2)
--noNimblePath
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config
