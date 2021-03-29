from controllers.coreController import Core

def main():
   '''
   :description: This method initialize the extraction and treat the arguments passed during this script initialization
   :return: None
   '''
   core = Core()
   core.initialize_extraction()



if __name__ == "__main__":
   main()