from services.minio.minio import save_raw_data
from utils.get_files_paths import select_files

def main():
    print("\033[1m\033[94m Load Data Service \n\033[0m")

    file_paths = select_files()

    if not file_paths:
        print("Nenhum arquivo selecionado.")
        return

    print(file_paths)

    save_raw_data(file_paths)

  # send to minio->s3

  # Parse urls for spark job

  # Run spark jobs

if __name__ == "__main__":
    main()
