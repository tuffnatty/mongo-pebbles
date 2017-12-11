
def configure(conf, env):
    print("Configuring pebbles storage engine module")
    if not conf.CheckCXXHeader("pebblesdb/db.h"):
        print("Could not find <pebblesdb/db.h>, required for PebblesDB storage engine build.")
        env.Exit(1)
