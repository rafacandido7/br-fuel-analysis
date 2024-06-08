from utils.get_files_paths import *

def main():
    print("\033[1m\033[94m Load Data Service \n\033[0m")

    file_paths = select_files()

    if not file_paths:
        print("Nenhum arquivo selecionado.")
        return

if __name__ == "__main__":
    main()
