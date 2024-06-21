from services.spark.__main__ import SparkAnalysisService
from rich.console import Console
from rich.prompt import Prompt

APP_NAME = "Spark Analysis"
URL = "jdbc:postgresql://db:5432/fuel_analysis"
DB_PROPERTIES = {
    "user": "root",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

console = Console()

def main():

    ascii_banner = """

      ██████╗ █████╗     ███████╗██████╗  █████╗ ██████╗ ██╗  ██╗     █████╗ ███╗   ██╗ █████╗ ██╗  ██╗   ██╗███████╗██╗███████╗
      ██╔════╝██╔══██╗    ██╔════╝██╔══██╗██╔══██╗██╔══██╗██║ ██╔╝    ██╔══██╗████╗  ██║██╔══██╗██║  ╚██╗ ██╔╝██╔════╝██║██╔════╝
      ██║     ███████║    ███████╗██████╔╝███████║██████╔╝█████╔╝     ███████║██╔██╗ ██║███████║██║   ╚████╔╝ ███████╗██║███████╗
      ██║     ██╔══██║    ╚════██║██╔═══╝ ██╔══██║██╔══██╗██╔═██╗     ██╔══██║██║╚██╗██║██╔══██║██║    ╚██╔╝  ╚════██║██║╚════██║
      ╚██████╗██║  ██║    ███████║██║     ██║  ██║██║  ██║██║  ██╗    ██║  ██║██║ ╚████║██║  ██║███████╗██║   ███████║██║███████║
      ╚═════╝╚═╝  ╚═╝    ╚══════╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝    ╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝╚═╝   ╚══════╝╚═╝╚══════╝
    """

    console.print(f"[bold blue]{ascii_banner}[/bold blue]")

    while True:
        try:
            console.print("[bold blue]Escolha o tipo de dados:[/bold blue]")
            console.print("[green]1. Dados dos Combustíveis[/green]")
            console.print("[green]2. Dados da cotação do dólar[/green]\n")

            data_choice = Prompt.ask("[blue]Digite sua escolha[/blue]", choices=["1", "2"])

            spark_analysis_service = SparkAnalysisService(APP_NAME, URL, DB_PROPERTIES)

            if data_choice == '1':
                input_paths = Prompt.ask("[bold blue]Passe o caminho dos arquivos, ou diretório, dos Dados de Combustíveis separados por ','[/bold blue]").split(',')
                input_paths = [path.strip() for path in input_paths]
                console.print("[blue]Processando Dados dos Combustíveis...[/blue]")
                spark_analysis_service.run_fuel_data(input_paths)
                console.print("[bold blue]Dados dos Combustíveis processados com sucesso![/bold blue]")
                break

            if data_choice == '2':
                input_paths = Prompt.ask("[green]Passe o caminho dos arquivos, ou diretório, dos Dados da cotação do dólar separados por ','[/green]").split(',')
                input_paths = [path.strip() for path in input_paths]
                console.print("[green]Processando Dados da cotação do dólar...[/green]")
                spark_analysis_service.run_dollar_data(input_paths)
                break

        except KeyboardInterrupt:
            console.print("[bold red]Programa interrompido pelo usuário.[/bold red]")
            return

if __name__ == "__main__":
    main()
