#!/usr/src/app/node_modules/.bin/ts-node

import { constants, createWriteStream } from 'fs';
import { resolve as resolvePath } from 'path';
import { Argv } from 'yargs';

import { ReadStream } from './directio';

function createReader(
	path: string,
	size: number | undefined,
	direct: boolean,
	numBuffers: number,
): ReadStream {
	return new ReadStream(path, direct, size, numBuffers);
}

async function flash(
	numBuffers: number,
	size: number | undefined,
	inputDirect: boolean,
	outputDirect: boolean,
	input: string,
	outputs: string[] = [],
) {
	const promises: Array<Promise<void>> = [];
	const source = createReader(input, size, inputDirect, numBuffers);
	source.setMaxListeners(outputs.length + 1);
	promises.push(
		new Promise((resolve, reject) => {
			source.on('close', resolve);
			source.on('error', reject);
		}),
	);
	const start = new Date().getTime();
	for (const output of outputs) {
		let flags = constants.O_WRONLY;
		if (outputDirect) {
			flags |= constants.O_DIRECT | constants.O_EXCL | constants.O_SYNC;
		}
		const destination = createWriteStream(output, {
			objectMode: true,
			highWaterMark: numBuffers - 1,
			// @ts-ignore (flags can be a number)
			flags,
		});
		const origWrite = destination._write.bind(destination);
		destination._write = (...args) => {
			// @ts-ignore
			console.log('write start', args[0].index);
			const origOnWrite = args[2]
			args[2] = (...aargs) => {
				// @ts-ignore
				origOnWrite(...aargs)
				console.log('write end', args[0].index);
			}
			// @ts-ignore
			return origWrite(...args);
		}
		promises.push(
			new Promise((resolve, reject) => {
				destination.on('close', resolve);
				destination.on('error', reject);
			}),
		);
		source.pipe(destination);
	}
	await Promise.all(promises);
	const end = new Date().getTime();
	const duration = (end - start) / 1000;
	if (size === undefined) {
		size = source.bytesRead;
	}
	console.log('total time', duration, 's');
	console.log('speed', size / 1024 ** 2 / duration, 'MiB/s');
}

const argv = require('yargs').command(
	'$0 input [devices..]',
	'Write zeros to devices',
	(yargs: Argv) => {
		yargs.positional('input', { describe: 'Input device' });
		yargs.positional('devices', { describe: 'Devices to write to' });
		yargs.option('numBuffers', {
			default: 2,
			describe: 'Number of 1MiB buffers used by the reader',
		});
		yargs.option('size', {
			type: 'number',
			describe: 'Size in bytes',
		});
		yargs.option('loop', {
			type: 'boolean',
			default: false,
			describe: 'Indefinitely restart flashing when done',
		});
		yargs.option('inputDirect', {
			type: 'boolean',
			default: false,
			describe: 'Use direct io for input',
		});
		yargs.option('outputDirect', {
			type: 'boolean',
			default: false,
			describe: 'Use direct io for output',
		});
	},
).argv;

async function main() {
	if (argv.devices === undefined || argv.devices.length === 0) {
		console.error('No output devices provided');
		return;
	}
	while (true) {
		await flash(
			argv.numBuffers,
			argv.size,
			argv.inputDirect,
			argv.outputDirect,
			resolvePath(argv.input),
			argv.devices.map((f: string) => resolvePath(f)),
		);
		if (!argv.loop) {
			break;
		}
	}
}

main();
