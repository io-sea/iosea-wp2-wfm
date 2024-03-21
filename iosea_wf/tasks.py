"""Module defining a set of useful tasks for CI/CD.
"""
import platform
import os
from pathlib import Path
from shutil import which
from invoke import task

__copyright__ = """
Copyright (C) Bull S. A. S.
"""

CURRENT_DIR = Path(__file__).parent.absolute()
FAKE_SBATCH = os.path.join(CURRENT_DIR, "tests", "test_bin", "fake_sbatch.sh")
SBATCH_SCRIPT = os.path.join(CURRENT_DIR, "tests", "test_bin", "sbatch_script.sh")
TEST_CONFIG_DIR = os.path.join(CURRENT_DIR, "tests", "test_cli", "test_data")
TEST_API_CONFIG = os.path.join(TEST_CONFIG_DIR, "api_settings.yaml")
TEST_API_CONFIG_WITHOUT_SLURM = os.path.join(TEST_CONFIG_DIR, "api_settings_no_slurm.yaml")

SRC_FOLDER = "iosea_wf"


def get_activate_venv():
    """Get the venv activation command depending on the OS.
    """
    if platform.system() == "Windows":
        return ".\\.env\\Scripts\\activate"

    return "source .env/bin/activate"


@task
def install_package(c,
                    extra="",
                    proxy="",
                    editable=False,
                    build_isolation=False):
    """Install a package"""
    install_extra = f"[{extra}]" if extra else ""
    install_through_proxies = f" --proxy {proxy}" if proxy else ""
    build_isolation_cmd = " --no-build-isolation" if not build_isolation else ""
    install_pkg = "pip install --no-cache-dir -e ." if editable else "pip install --no-cache-dir ."
    # Upgrade setuptools
    c.run("pip install --upgrade setuptools" + install_through_proxies)
    # Install package
    c.run(install_pkg + install_extra +
          install_through_proxies + build_isolation_cmd)


@task
def install(c,
            extra="",
            proxy="",
            editable=False,
            build_isolation=False,
            venv=False):
    """Install the package

    Args:
        proxies (str, optional): _description_. Defaults to "".
        editable (bool, optional): _description_. Defaults to False.
    """
    if venv:
        venv_cmd = "virtualenv .env"
        c.run(venv_cmd)
        activate_cmd = get_activate_venv()
        with c.prefix(activate_cmd):
            install_package(c,
                            extra=extra,
                            proxy=proxy,
                            editable=editable,
                            build_isolation=build_isolation)
    else:
        install_package(c,
                        extra=extra,
                        proxy=proxy,
                        editable=editable,
                        build_isolation=build_isolation)


@task
def test(c, coverage=True, venv=False, report=""):
    """Run the unit tests of the package.
    """
    cov = f"--cov={SRC_FOLDER}" if coverage else ""
    report_cmd = f" --cov-report {report}" if report else ""
    test_cmd = f"pytest {cov}{report_cmd} tests"
    kill_cmd = "kill -9 $(ps -C wfm-api -o pid=)"
    api_config = TEST_API_CONFIG
    fake_sbatch = FAKE_SBATCH
    c.run(f"chmod +x {fake_sbatch}; cp -pf {fake_sbatch} /tmp/")
    sbatch_script = SBATCH_SCRIPT
    c.run(f"chmod +x {sbatch_script}; cp -pf {sbatch_script} /tmp/")
    if venv:
        activate_cmd = get_activate_venv()
        with c.prefix(activate_cmd):
            if which('scontrol') is None:
                api_config = TEST_API_CONFIG_WITHOUT_SLURM
            c.run(command=f"wfm-api --settings {api_config}", asynchronous=True, echo=True)
            c.run("echo wfm_api starting in venv...")
            c.run("sleep 5")
            try:
                c.run(test_cmd)
            except:
                c.run(command=kill_cmd, asynchronous=True, echo=True)
                return 1
    else:
        if which('scontrol') is None:
            api_config = TEST_API_CONFIG_WITHOUT_SLURM
        c.run(command=f"wfm-api --settings {api_config}", asynchronous=True, echo=True)
        c.run("echo wfm_api starting...")
        c.run("sleep 5")
        try:
            c.run(test_cmd)
        except:
            c.run(command=kill_cmd, asynchronous=True, echo=True)
            return 1
    c.run(command=kill_cmd, asynchronous=True, echo=True)


@task
def lint(c, rc_file="", output_file="", venv=False):
    """Lint the package using pylint.
    """
    rc_cmd = f" --rcfile={rc_file}" if rc_file else ""
    output_file_cmd = f" > {output_file}" if output_file else ""
    lint_cmd = "pylint --recursive=y --exit-zero --output-format=parseable --reports=no "\
        f"{SRC_FOLDER} {rc_cmd} {output_file_cmd}"
    if venv:
        activate_cmd = get_activate_venv()
        with c.prefix(activate_cmd):
            c.run(lint_cmd)
    else:
        c.run(lint_cmd)


@task
def build(c,
          version="0.0.0",
          proxy="",
          outdir="dist",
          with_deps=False,
          deps_dir="deps"):
    """Build the Python package.

    Args:
        version (str, optional): The version to tag the built wheel with.
            Defaults to 0.0.0.
        proxy (str, optional): The proxy to use for installation.
            Defaults to not using any proxy.
        outdir (str, optional): The path to the folder to store the wheel.
            Defaults to dist.
        with_deps (bool, optional): Whether or not to bundle the dependencies when
            building the package. If set to yes, then the output will be a tar file
            with a folder deps and the wheel within it.
        deps_dir (str, optional): Where to set the downloaded dependencies. As a default,
            will be stored in a folder "deps".
    """
    outdir_cmd = f" --outdir {outdir}" if outdir else ""
    install_through_proxies = f" --proxy {proxy}" if proxy else ""
    # Set version value in version.py
    c.run(f"sed -e 's/__version__.*/__version__=\"{version}\"/' -i {SRC_FOLDER}/version.py")
    # Set version value in pyproject.toml
    c.run(f"sed -e 's/version.*/version=\"{version}\"/' -i pyproject.toml")
    # Create Python venv and activate
    c.run("virtualenv .env")
    activate_cmd = get_activate_venv()
    with c.prefix(activate_cmd):
        c.run("pip install --upgrade build pip setuptools" + install_through_proxies)
        build_cmd = "python -m build --no-isolation" + outdir_cmd
        c.run(build_cmd)
        if with_deps:
            # Download deps into folder
            c.run(f"pip download . --dest {outdir}/{deps_dir}")


@task
def run_integration_tests(c,
                          run_api=True,
                          coverage=False):
    """Run the integration tests of the platform.

    Args:
        run_api (bool, optional): Whether or not to run the wfm_api in background
            Defaults to True.
        coverage (bool, optional): Whether or not to output the coverage.
    """
    if run_api:
        c.run(command=f"wfm-api --settings {TEST_API_CONFIG}", asynchronous=True, echo=True)
    cov = f"--cov={SRC_FOLDER}" if coverage else ""
    c.run(f"pytest {cov} tests")
