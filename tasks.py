"""Module defining a set of useful tasks for CI/CD.
"""
import platform
import fileinput
from typing import List
from pathlib import Path
from invoke import task


PACKAGE_LIST = ["wfm_api", "iosea_wf"]


def get_mv_cmd():
    """Get the right command to move files depending on the OS.
    """
    if platform.system() == "Windows":
        return "move"
    else:
        return "mv"


def get_activate_venv():
    """Get the venv activation command depending on the OS.
    """
    if platform.system() == "Windows":
        return ".\.env\Scripts\\activate"
    else:
        return "source .env/bin/activate"


def merge_logs(files: List[str], output_file: str):
    """Merge a list of files into a single one.

    Args:
        files (List[str]): The list of the files to concatenate as
            a list of path as strings.
        output_file (str): The path to the final file.
    """
    with open(output_file, 'w', encoding="utf-8") as file:
        input_lines = fileinput.input(files)
        file.writelines(input_lines)


@task
def install_build_environment(c, proxy=""):
    """Install all of the packages.

    Args:
        c (_type_): _description_
        proxy (str, optional): proxy needed to get the required packages. Defaults to "".
    """
    install_dependencies_cmd = "pip install -r cip-requirements.txt"
    if proxy:
        install_dependencies_cmd += "--proxy {proxy}"
    c.run(install_dependencies_cmd)


@task
def install(c,
            extra="",
            proxy="",
            editable=False,
            build_isolation=False,
            venv=False):
    """Install all of the packages.

    Args:
        c (_type_): _description_
        extra (str, optional): _description_. Defaults to "".
        proxy (str, optional): proxy needed to get the required packages. Defaults to "".
        editable (bool, optional): editable installation. Defaults to False.
        build_isolation (bool, optional): whether to build in isolation mode. Defaults to False.
        venv (bool, optional): whether to build in a venv. Defaults to False.
    """
    extra_cmd = f"--extra {extra}" if extra else ""
    editable_cmd = "--editable" if editable else ""
    proxy_cmd = f"--proxy {proxy}" if proxy else ""
    build_isolation_cmd = "--build-isolation" if build_isolation else ""
    if venv:
        # Remove existing virtual environment
        c.run("rm -rf .env")
        # Create new venv
        venv_cmd = "virtualenv .env"
        c.run(venv_cmd)
        activate_cmd = get_activate_venv()
        with c.prefix(activate_cmd):
            c.run("pip install --upgrade pip setuptools")
            for package in PACKAGE_LIST:
                c.run(f"cd {package} && \
                    invoke install {editable_cmd} {proxy_cmd} {extra_cmd} {build_isolation_cmd}")
    else:
        c.run("pip install --upgrade pip setuptools")
        for package in PACKAGE_LIST:
            c.run(f"cd {package} && \
                invoke install {editable_cmd} {proxy_cmd} {extra_cmd} {build_isolation_cmd}")


@task
def test(c, coverage=True, venv=False, report=""):
    """Run the unit tests of the packages.
    """
    cov = "--coverage" if coverage else ""
    for package in PACKAGE_LIST:
        # Run tests
        if venv:
            activate_cmd = get_activate_venv()
            with c.prefix(activate_cmd):
                c.run(f"cd {package} && invoke test {cov}")
        else:
            c.run(f"cd {package} && invoke test {cov}")
        # If reports are generated, move them to the main folder
        if report:
            c.run(f"{get_mv_cmd()} {Path(package) / '.coverage'} .coverage-{package}")
    # If reports are generated, combine them and generate xml
    if report:
        report_names = " ".join([f".coverage-{package}" for package in PACKAGE_LIST])
        c.run(f"coverage combine {report_names} && coverage xml -o {report}")


@task
def lint(c, rc_file="", output_file="", venv=True):
    """Lint the packages using pylint.
    """
    rc_cmd = f" --rc-file={rc_file}" if rc_file else ""
    output_cmd = f" --output-file={output_file}" if output_file else ""
    for package in PACKAGE_LIST:
        lint_cmd = f"cd {package} && invoke lint {output_cmd} {rc_cmd}"
        if venv:
            activate_cmd = get_activate_venv()
            with c.prefix(activate_cmd):
                c.run(lint_cmd)
        else:
            c.run(lint_cmd)
        # Move log if generated
        if output_file:
            c.run(f"{get_mv_cmd()} {Path(package) / output_file} pylint-{package}.log")
    # If log is generated, output all into output_file
    if output_file:
        logs = [f"pylint-{package}.log" for package in PACKAGE_LIST]
        merge_logs(files=logs, output_file=output_file)


@task
def build(c,
          version="0.0.0",
          proxy="",
          outdir="",
          with_deps=False):
    """Bundle the Python code into a wheel.

    Args:
        version (str, optional): The version to tag the built wheel with.
            Defaults to 0.0.0.
        proxy (str, optional): The proxy to use for installation.
            Defaults to not using any proxy.
        outdir (str, optional): The path to the folder to store the wheel.
            Defaults to dist.
        with_deps (bool): Whether or not to download dependencies.
    """
    outdir_cmd = f" --outdir {outdir}" if outdir else ""
    install_through_proxies = f" --proxy {proxy}" if proxy else ""
    with_deps_cmd = " --with-deps " if with_deps else ""
    version_cmd = f" --version {version} "
    for package in PACKAGE_LIST:
        c.run(
            f"cd {package} &&"
            f"invoke build {outdir_cmd} {install_through_proxies} {with_deps_cmd} {version_cmd}")


@task
def bundle(c,
           build_dir="",
           build_folder="dist",
           archive_name="iosea_all"):
    """Bundle all generated wheels and dependencies into a single archive.

    Args:
        build_dir (str): The path where the built packages are located.
        build_folder (str): The name of the folder to tar.
        archive_name (str): The name of the archive that will contain all the packages builds.
    """
    c.run(f"tar -cpzf {archive_name}.tar.gz --directory={build_dir} {build_folder}")


@task
def doc(c):
    """Build documentation.
    """
    for package in PACKAGE_LIST:
        c.run(f"cd {package} && invoke doc")
