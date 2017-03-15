import os
import shutil
import tempfile

import yaml

from xpdan.glbl import logger
from xpdan.simulation import build_pymongo_backed_broker
import logging

logger = logging.getLogger(__name__)


def load_configuration(name, prefix, fields):
    """
    Load configuration data from a cascading series of locations.

    The precedence order is (highest priority last):

    1. The conda environment
       - CONDA_ENV/etc/{name}.yaml (if CONDA_ETC_ is defined for the env)
    2. At the system level
       - /etc/{name}.yml
    3. In the user's home directory
       - ~/.config/{name}/connection.yml
    4. Environmental variables
       - {PREFIX}_{FIELD}

    where
        {name} is metadatastore
        {PREFIX} is MDS
        {FIELD} is one of {host, database, port, timezone}

    Parameters
    ----------
    name : str
        The expected base-name of the configuration files

    prefix : str
        The prefix when looking for environmental variables

    fields : iterable of strings
        The required configuration fields

    Returns
    ------
    conf : dict
        Dictionary keyed on ``fields`` with the values extracted
    """
    filenames = [
        os.path.join('/etc', name + '.yml'),
        os.path.join(os.path.expanduser('~'), '.config', name,
                     '.yml'),
    ]

    if 'CONDA_ETC_' in os.environ:
        filenames.insert(0, os.path.join(
            os.environ['CONDA_ETC_'], name + '.yml'))

    config = {}
    for filename in filenames:
        if os.path.isfile(filename):
            with open(filename) as f:
                config.update(yaml.load(f))
            logger.debug("Using glbl specified in config file. \n%r",
                         config)

    for field in fields:
        var_name = prefix + '_' + field.upper().replace(' ', '_')
        config[field] = os.environ.get(var_name, config.get(field, None))
        # Valid values for 'port' are None or castable to int.
        if field == 'port' and config[field] is not None:
            config[field] = int(config[field])

    missing = [k for k, v in config.items() if v is None]
    if missing:
        raise KeyError("The configuration field(s) {0} "
                       "were not found in any file or environmental "
                       "variable.".format(missing))
    return config


def make_glbl(config, env_code=0, db_xptal=None, db_an=None):
    """ make a instance of Glbl class

    Glbl class is used to handle attributes and directories
     depends on environment variable

    Parameters
    ----------
    env_code : int
        environment variable to specify current situation

    Note
    ----
    by default: env_var 0 means beamline, 1 means test, 2 means simulation
    """

    # change this to be handled by an environment variable later
    if int(env_code) == 1:
        # test
        base_dir = tempfile.mkdtemp()
        print('creating {}'.format(base_dir))
    elif int(env_code) == 2:
        # simulation
        base_dir = os.getcwd()
        db_xptal = build_pymongo_backed_broker()
    else:
        # beamline
        base_dir = os.path.abspath('/direct/XF28ID1/pe2_data')
        from databroker.databroker import DataBroker as db_xptal

    config['base_dir'] = base_dir
    config['exp_db'] = db_xptal
    config['an_db'] = db_an
    # top directories
    config.update({k: os.path.join(config['base_dir'], config[z]) for k, z in
                   zip(['home_dir', 'blconfig_dir'],
                       ['home_dir_name', 'blconfig_dir_name'])})
    config['archive_base_dir'] = os.path.abspath(
        config['archive_base_dir_name'])

    # aquire object directories
    config.update(
        dict(config_base=os.path.join(config['home_dir'], 'config_base')))

    # copying pyFAI calib dict yml for test
    if int(env_code) == 1:
        a = os.path.dirname(os.path.abspath(__file__))
        b = a.split('glbl.py')[0]
        os.makedirs(config['config_base'], exist_ok=True)
        shutil.copyfile(os.path.join(b, 'tests/pyFAI_calib.yml'),
                        os.path.join(config['config_base'], 'pyFAI_calib.yml'))

    config.update(
        dict(yaml_dir=os.path.join(config['home_dir'], 'config_base', 'yml')))
    config.update({k: os.path.join(config['home_dir'], z) for k, z in zip(
        ['import_dir', 'analysis_dir', 'userscript_dir',
         'tiff_base'],
        ['Import', 'userAnalysis', 'userScripts', 'tiff_base']
    )})

    config['bt_dir'] = config['yaml_dir']
    config.update({k: os.path.join(config['yaml_dir'], z) for k, z in zip(
        ['sample_dir', 'experiment_dir' 'scanplan_dir'],
        ['samples', 'experiments', 'scanplans']
    )})
    # other dirs
    config['user_backup_dir'] = os.path.join(config['archive_base_dir'],
                                             config['user_backup_dir_name'])

    config['all_folders'] = [config[k] for k in ['home_dir',
                   'blconfig_dir',
                   'yaml_dir',
                   'config_base',
                   'sample_dir',
                   'experiment_dir',
                   'scanplan_dir',
                   'tiff_base',
                   'userscript_dir',
                   'import_dir',
                   'analysis_dir']]

    # only create dirs if running test
    if int(env_code) == 1:
        for folder in config['all_folders']:
            os.makedirs(folder, exist_ok=True)

    # directories that won't be tar in the end of beamtime
    config['_exclude_dir'] = [config[k] for k in ['home_dir', 'blconfig_dir', 'yaml_dir']]
    config['_export_tar_dir'] = [config[k] for k in ['config_base', 'userscript_dir']]
    return config