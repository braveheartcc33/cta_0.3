import argparse

from cta_controller import CTAController

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fundingrate arbitrage")

    parser.add_argument("config_path", type=str, help="配置文件的路径。")

    args = parser.parse_args()

    config_path = args.config_path

    controller = CTAController(config_path)
