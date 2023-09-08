from my_rpc.server import DataTransformService
    
if __name__ == '__main__':
    server = DataTransformService()
    print("Server has started", flush=True)
    server.serve()


