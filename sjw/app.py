import argparse
import logging
import logging.config
from functools import wraps
from pathlib import Path
from typing import NamedTuple

import toml
from autobahn.twisted.component import Component
from twisted.internet.defer import ensureDeferred
from twisted.internet.task import react
from twisted.logger import globalLogBeginner, STDLibLogObserver
from txdbus import client

logger = logging.getLogger(__name__)
here_path = Path(__file__).parent


class Unit(NamedTuple):
    name: str
    description: str
    active: str
    sub: str
    enabled: str


def main(args=None):
    parser = argparse.ArgumentParser(
        description='swj: systemctl and journalctl wrapper')
    parser.add_argument('-c', '--config', required=True,
                        type=argparse.FileType('r'),
                        help='Configuration file')
    args = parser.parse_args(args)
    return App(config_file=args.config).run()


def to_deferred(func):
    @wraps(func)
    def wrapper(*args, **kwds):
        return ensureDeferred(func(*args, **kwds))
    return wrapper


class WAMPApplication:
    def __init__(self, app, config):
        self.app = app
        self.wamp_session = None
        self.wamp_comp = Component(
            transports=config['router'],
            realm=config['realm']
        )
        self.wamp_comp.on('join', self.initialize)
        self.wamp_comp.on('leave', self.uninitialize)

    @to_deferred
    async def initialize(self, session, details):
        logger.info("Connected to WAMP router")
        self.wamp_session = session
        await session.register(self.app.list_units, 'sjw.list_units')
        await session.register(self.app.query, 'sjw.query')
        await session.register(self.app.start, 'sjw.start')
        await session.register(self.app.stop, 'sjw.stop')
        await session.register(self.app.enable, 'sjw.enable')
        await session.register(self.app.disable, 'sjw.disable')

    def uninitialize(self, session, reason):
        logger.info("%s %s", session, reason)
        logger.info("Lost WAMP connection")
        self.wamp_session = None

    def on_unit_changed(self, unit, props):
        if not self.wamp_session:
            return
        self.wamp_session.publish('sjw.unit.' + unit, props)

    @to_deferred
    async def start(self, reactor=None):
        logger.info("Starting component")
        return (await self.wamp_comp.start(reactor))


class App:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.load_config(self.config_file)
        self.wamp = WAMPApplication(self, self.config['wamp'])
        self.connection = None      # D-Bus connection
        self.systemd_dbus = None
        self.units = {}
        self.units_name = self.config['units']
        self.units_path = {}
        self.properties = {}

    @staticmethod
    def load_config(file):
        if isinstance(file, (str, Path)):
            file = open(file)
        with file:
            config = toml.load(file)
        return config

    def run(self):
        config = self.config
        observers = [STDLibLogObserver()]
        globalLogBeginner.beginLoggingTo(observers, redirectStandardIO=False)
        logging.config.dictConfig(config['logging'])
        logging.captureWarnings(True)
        logger.info('Logging configured!')
        return react(self.main_loop)

    @to_deferred
    async def main_loop(self, reactor):
        self.connection = await client.connect(reactor, 'session')
        self.systemd_dbus = await self.connection.getRemoteObject(
            busName='org.freedesktop.systemd1',
            objectPath='/org/freedesktop/systemd1'
        )
        await self.systemd_dbus.callRemote('Subscribe')
        await self.refresh_units()
        for key, path in self.units_path.items():
            logger.debug('%s: %s', key, path)
        await self.refresh_unit_properties()
        await self.subscribe_props_units()
        result = await self.wamp.start(reactor)
        logger.info("WAMPApplication is done: %s", result)

    async def refresh_units(self, filter_keys=None):
        for key, unit_name in self.units_name.items():
            if not filter_keys or key in filter_keys:
                # Get paths (this is needed for unloaded units)
                self.units_path[key] = await self.systemd_dbus.callRemote(
                    'LoadUnit', unit_name)
                # Get properties
                unit_dbus = await self.connection.getRemoteObject(
                    'org.freedesktop.systemd1', self.units_path[key])
                props = await unit_dbus.callRemote('GetAll', '')
                self.properties[key] = props
                self.units[key] = Unit(name=unit_name,
                                       description=props['Description'],
                                       active=props['ActiveState'],
                                       sub=props['SubState'],
                                       enabled=props['UnitFileState'])

    async def subscribe_props_units(self):
        for unit_key, unit_path in self.units_path.items():
            unit_dbus = await self.connection.getRemoteObject(
                'org.freedesktop.systemd1', unit_path)

            def unit_changed(interface, changed, invalidated):
                logger.debug("Unit changed: %s (%s)", unit_key, interface)
                self.properties.setdefault(unit_key, {}).update(changed)
                self.wamp.on_unit_changed(unit_key, changed)

            unit_dbus.notifyOnSignal('PropertiesChanged', unit_changed)

    async def refresh_unit_properties(self, unit_keys=None):
        if not unit_keys:
            unit_keys = self.units_path.keys()
        for unit_key in unit_keys:
            unit_dbus = await self.connection.getRemoteObject(
                'org.freedesktop.systemd1', self.units_path[unit_key])
            props = await unit_dbus.callRemote('GetAll', '')
            self.properties[unit_key] = props

    @to_deferred
    async def list_units(self):
        await self.refresh_units()
        def to_dict(unit):
            return {
                'Name': unit.name,
                'Description': unit.description,
                'ActiveState': unit.active,
                'SubState': unit.sub,
                'UnitFileState': unit.enabled
            }
        return {k: to_dict(v) for k, v in self.units.items()}

    @to_deferred
    async def query(self, *unit_keys):
        if not unit_keys:
            unit_keys = self.units_path
        await self.refresh_unit_properties(unit_keys)
        return {k: self.properties[k] for k in unit_keys
                if k in self.properties}

    @to_deferred
    async def start(self, unit_key):
        unit_name = self.units_name[unit_key]
        result = await self.systemd_dbus.callRemote(
            'StartUnit', unit_name, 'replace')
        return result

    @to_deferred
    async def stop(self, unit_key):
        unit_name = self.units_name[unit_key]
        result = await self.systemd_dbus.callRemote(
            'StopUnit', unit_name, 'replace')
        return result

    @to_deferred
    async def enable(self, unit_key):
        unit_name = self.units_name[unit_key]
        result = await self.systemd_dbus.callRemote(
            'EnableUnitFiles', [unit_name], False, True)
        await self.systemd_dbus.callRemote('Reload')
        return result

    @to_deferred
    async def disable(self, unit_key):
        unit_name = self.units_name[unit_key]
        result = await self.systemd_dbus.callRemote(
            'DisableUnitFiles', [unit_name], False)
        await self.systemd_dbus.callRemote('Reload')
        return result
