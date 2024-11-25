import subprocess
import pkg_resources
import sys
from typing import List, Tuple
import argparse
from datetime import datetime

def get_outdated_packages() -> List[Tuple[str, str, str]]:
    """Get list of outdated packages with their versions"""
    outdated = []
    for dist in pkg_resources.working_set:
        latest = None
        try:
            latest = subprocess.check_output(
                [sys.executable, '-m', 'pip', 'install', '{}==random'.format(dist.key)],
                stderr=subprocess.STDOUT
            ).decode('utf-8')
        except subprocess.CalledProcessError as e:
            latest = e.output.decode('utf-8')

        if 'versions: ' in latest:
            latest_version = latest.split('versions: ')[-1].split(',')[0].strip()
            if latest_version != dist.version:
                outdated.append((dist.key, dist.version, latest_version))

    return outdated

def update_package(package: str, specific_version: str = None) -> bool:
    """Update a specific package"""
    try:
        if specific_version:
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install',
                f'{package}=={specific_version}'
            ])
        else:
            subprocess.check_call([
                sys.executable, '-m', 'pip', 'install',
                f'{package}', '--upgrade'
            ])
        return True
    except subprocess.CalledProcessError:
        return False

def backup_requirements():
    """Backup current requirements.txt"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    try:
        subprocess.check_call([
            sys.executable, '-m', 'pip', 'freeze',
            '>', f'requirements_backup_{timestamp}.txt'
        ], shell=True)
        return True
    except subprocess.CalledProcessError:
        return False

def update_requirements():
    """Update requirements.txt with current package versions"""
    try:
        subprocess.check_call([
            sys.executable, '-m', 'pip', 'freeze',
            '>', 'requirements.txt'
        ], shell=True)
        return True
    except subprocess.CalledProcessError:
        return False

def main():
    parser = argparse.ArgumentParser(description='Package Update Utility')
    parser.add_argument('--package', help='Specific package to update')
    parser.add_argument('--version', help='Specific version to install')
    parser.add_argument('--all', action='store_true', help='Update all packages')
    parser.add_argument('--backup', action='store_true', help='Backup requirements before updating')

    args = parser.parse_args()

    if args.backup:
        print("Backing up current requirements...")
        if backup_requirements():
            print("Backup created successfully")
        else:
            print("Failed to create backup")
            return

    if args.package:
        print(f"Updating {args.package}...")
        if update_package(args.package, args.version):
            print(f"Successfully updated {args.package}")
        else:
            print(f"Failed to update {args.package}")

    elif args.all:
        print("Checking for outdated packages...")
        outdated = get_outdated_packages()

        if not outdated:
            print("All packages are up to date!")
            return

        print("\nOutdated packages:")
        for package, current, latest in outdated:
            print(f"{package}: {current} -> {latest}")

        input("\nPress Enter to continue with updates...")

        for package, _, _ in outdated:
            print(f"\nUpdating {package}...")
            if update_package(package):
                print(f"Successfully updated {package}")
            else:
                print(f"Failed to update {package}")

    print("\nUpdating requirements.txt...")
    if update_requirements():
        print("requirements.txt updated successfully")
    else:
        print("Failed to update requirements.txt")

if __name__ == "__main__":
    main()
