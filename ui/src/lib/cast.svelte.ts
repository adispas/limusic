// Google Cast: play to a Chromecast, a Google TV or a Nest speaker (src-tauri/src/cast). Devices are
// looked for only while the picker is open; the session lives in Rust and survives a UI reload.
import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import { t, type TranslationKey } from './i18n.svelte';
import { toast } from './player.svelte';

export interface CastDevice {
	id: string;
	name: string;
	/** "Google Nest Mini", "Chromecast", "Google Cast Group"… */
	model: string;
}

export const cast = $state<{ devices: CastDevice[]; connected: CastDevice | null; connecting: string | null }>({
	devices: [],
	connected: null,
	connecting: null
});

/** Rust reports known problems as codes; anything else is already a sentence. */
const errorText = (message: string) => {
	const key = `cast.errors.${message}` as TranslationKey;
	const text = t(key);
	return text === key ? message : text;
};

export const castDiscover = (on: boolean) => invoke<void>('cast_discover', { on }).catch(() => {});

export async function castConnect(device: CastDevice) {
	if (cast.connecting) return;
	cast.connecting = device.id;
	try {
		await invoke<void>('cast_connect', { id: device.id });
	} catch (e) {
		toast.error(t('cast.could_not_connect', { name: device.name, reason: errorText(String(e)) }));
	} finally {
		cast.connecting = null;
	}
}

export const castDisconnect = () => invoke<void>('cast_disconnect').catch(() => {});

/** Speakers get a speaker icon, everything with a screen a TV. */
export const isSpeaker = (d: CastDevice) =>
	/nest|home|speaker|audio|mini|max|hub|group/i.test(d.model) && !/tv|chromecast/i.test(d.model);

/** Subscribe to the Rust side and seed the current state (from `initPlayer`). */
export function initCast(): Promise<UnlistenFn> {
	invoke<{ devices: CastDevice[]; connected: CastDevice | null }>('cast_status')
		.then((s) => {
			cast.devices = s.devices;
			cast.connected = s.connected;
		})
		.catch(() => {});
	const subs = [
		listen<CastDevice[]>('cast-devices', (e) => (cast.devices = e.payload)),
		listen<{ connected: CastDevice | null }>('cast-session', (e) => (cast.connected = e.payload.connected)),
		listen<{ message: string }>('cast-error', (e) => toast.error(errorText(e.payload.message)))
	];
	return Promise.all(subs).then((fns) => () => fns.forEach((f) => f()));
}
