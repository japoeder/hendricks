import debugpy

debugpy.listen(5678)
debugpy.wait_for_client()

def bp():
    return debugpy.breakpoint()