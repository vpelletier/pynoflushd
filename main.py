#!/usr/bin/env python
from time import sleep, time, ctime
from fcntl import ioctl
import os
import subprocess
import re

VERSION = '0.1'

DISK_STATE_RE = re.compile(r'^ drive state is:(.+)')
OVERSLEEP_FACTOR = 3
BLKFLSBUF = 0x00001261 # <include/linux/fs.h>
MIN_FLUSH_PERIOD = 500 # in ms
PDFLUSH_PATH = '/proc/sys/vm/dirty_writeback_centisecs'
class PDFlush(object):
    _stopped = False

    def __init__(self):
        self._initial_interval = self.getInterval()

    def getInitialInterval(self):
        return self._initial_interval

    def start(self):
        self._setInterval(self._initial_interval)
        self._stopped = False

    def stop(self):
        delay = self.getInterval()
        if delay:
            self._stopped = True
            self._setInterval(0)

    @staticmethod
    def _setInterval(value):
        control = open(PDFLUSH_PATH, 'w')
        control.write('%i' % (value, ))
        control.flush()
        control.close()

    @staticmethod
    def getInterval():
        control = open(PDFLUSH_PATH)
        result = int(control.read())
        control.close()
        return result

    def __del__(self):
        if self._stopped:
            self.start()

_disk_map = {}
def BlockDevice(disk):
    try:
        result = _disk_map[disk]
    except KeyError:
        _disk_map[disk] = result = _BlockDevice(disk)
    return result

class _BlockDevice(object):
    controlled = False
    next_spindown = None

    def __init__(self, disk):
        self._disk = disk
        self._sys = sys = '/sys/block/' + disk
        self._stat = sys + '/stat'
        self._rotational = sys + '/queue/rotational'
        self._dev = '/dev/' + disk
        self._spinning = True
        self._last_read_stat = None
        self._last_write_stat = None
        self._dependent_block_device_list = []
        for path in (self._stat, self._rotational, self._dev):
            if not os.path.exists(path):
                raise Exception('Non-existant path %r' % (path, ))
        self.refreshDepList()

    def refreshDepList(self):
        deplist = self._dependent_block_device_list
        del deplist[:]
        slaves_path = self._sys + '/slaves'
        if os.path.exists(slaves_path):
            append = deplist.append
            for slave in os.listdir(slaves_path):
                state_path = self._sys + '/md/dev-' + slave + '/state'
                assert os.path.exists(state_path), state_path
                if open(state_path).read() == 'spare':
                    # Spare disks should not prevent us from flushing when
                    # they are spun down, as nothing will be written to them.
                    continue
                slave_path = os.path.realpath(slaves_path + '/' + slave)
                while not os.path.exists(slave_path + '/device'):
                    split_slave = slave_path.split('/')[:-1]
                    assert len(split_slave) > 1, self
                    slave_path = '/'.join(split_slave)
                append(BlockDevice(os.path.basename(slave_path)))

    def __repr__(self):
        return self._disk

    @property
    def spinning(self):
        if self._dependent_block_device_list:
            for dependency in self._dependent_block_device_list:
                if not dependency.spinning:
                    # If any dependent drive isn't spinning, consider us as not
                    # spinning, so we don't get flushed.
                    return False
            return True
        else:
            if int(open(self._rotational).read()):
                hdparm = subprocess.Popen(['hdparm', '-C', self._dev],
                    stdout=subprocess.PIPE)
                hdparm.wait()
                output = hdparm.stdout.read()
                for line in output.splitlines():
                    matched = DISK_STATE_RE.match(line)
                    if matched:
                        spinning = matched.group(1).strip() not in (
                            'standby', 'sleeping')
                        break
                else:
                    print 'Warning: could not understand hdparm -C output. ' \
                        'Using internal state tracking.', 'Output was:', repr(
                            output)
                    spinning = self._spinning
            else:
                # Consider non-rotating disks as always spinning. Ie, never
                # delay flushes to SSDs.
                spinning = True
            return spinning

    def getTargetSpin(self):
        return self._spinning

    def updateReadStats(self):
        if self._dependent_block_device_list:
            raise ValueError('Should only be called on raw devices')
        statfile = open(self._stat)
        rio, _, rblk, _, wio, _, wblk, _, _, _, _ = [int(x.strip()) \
            for x in statfile.read().split()]
        statfile.close()
        read_stat = (rio, rblk)
        write_stat = (wio, wblk)
        read_happened = read_stat != self._last_read_stat
        write_happened = write_stat != self._last_write_stat
        self._last_read_stat = read_stat
        self._last_write_stat = write_stat
        if read_happened or write_happened:
            self._spinning = True
        return read_happened, write_happened

    def spinDown(self):
        if self._dependent_block_device_list:
            raise ValueError('Should only be called on raw devices')
        self.flush()
        self.flush()
        success = not self.updateReadStats()[0]
        if success:
            os.system('hdparm -qy ' + self._dev)
            self._spinning = False
        return success

    def flush(self):
        try:
            device = open(self._dev)
        except IOError, exc:
            if exc.errno != 123: # no medium found
                raise
        else:
            ioctl(device.fileno(), BLKFLSBUF)
            device.close()

def main(disk_list, default_timeout):
    if not disk_list:
        raise ValueError('no disk_list')
    if not default_timeout or default_timeout < 0:
        raise ValueError('invalid timrout: %r' % (default_timeout, ))
    # TODO: support more noflushd features
    # TODO: stop printing
    flusher = PDFlush()
    sync_period = max(flusher.getInitialInterval(), MIN_FLUSH_PERIOD) / 100.
    flusher.stop()
    while True:
        # Refresh device list on every iteration, so we don't risk forgetting
        # a flush
        complete_device_list = []
        append = complete_device_list.append
        for device in os.listdir('/sys/block'):
            bdev = BlockDevice(device)
            bdev.controlled = device in disk_list
            append(bdev)
        now = time()
        next_spindown = now + default_timeout
        for disk in complete_device_list:
            disk.refreshDepList()
            if disk.controlled:
                target_spin = disk.getTargetSpin()
                read_happened, write_happened = disk.updateReadStats()
                if read_happened or (not target_spin and write_happened):
                    # I/O are flowing, disk is spinning and next spin down is
                    # delayed
                    if not target_spin:
                        print ctime(), 'spin up', disk
                    disk.next_spindown = next_spindown
                elif target_spin and disk.next_spindown <= now:
                    print ctime(), 'spindown', disk
                    if not disk.spinDown():
                        print '...failed, rescheduling'
                        disk.next_spindown = next_spindown
                if disk.spinning:
                    disk.flush()
        for disk in complete_device_list:
            if not disk.controlled:
                if disk.spinning:
                    disk.flush()
        while True:
            sleep(sync_period)
            new_now = time()
            if new_now < now + sync_period * OVERSLEEP_FACTOR:
                break
            # We overslept ! Maybe host got suspended. Or it is overloaded.
            # In any case, delay next sleep.
            now = new_now

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser(epilog='Follow with any block device you wish '
        'pynoflushd to control. Use device actual basename (eg: "sda") only.')
    parser.add_option('-n', type='float', dest='default_timeout',
        help='Default timeout for spindown in minutes')
    (options, args) = parser.parse_args()
    main(
        disk_list=args,
        default_timeout=options.default_timeout * 60,
    )

