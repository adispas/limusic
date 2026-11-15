<script lang="ts">
	// Cast to a speaker or a TV on the same network (src-tauri/src/cast). Lit while casting; looks
	// for devices only while open.
	import { HugeiconsIcon } from '@hugeicons/svelte';
	import { CastIcon, ComputerIcon, Speaker01Icon, Tv01Icon, VolumeHighIcon } from '@hugeicons/core-free-icons';
	import * as Popover from '$lib/components/ui/popover';
	import { cast, castConnect, castDisconnect, castDiscover, isSpeaker, type CastDevice } from '$lib/cast.svelte';
	import { t } from '$lib/i18n.svelte';

	let open = $state(false);
	// After a while with nothing found, say what to check instead of spinning on.
	let slow = $state(false);

	$effect(() => {
		void castDiscover(open);
		slow = false;
		if (!open) return;
		const id = setTimeout(() => (slow = true), 8000);
		return () => clearTimeout(id);
	});

	const label = $derived(cast.connected ? t('cast.casting_to', { name: cast.connected.name }) : t('cast.cast'));

	function pick(d: CastDevice) {
		if (cast.connected?.id === d.id) {
			open = false;
			return;
		}
		void castConnect(d).then(() => {
			if (cast.connected?.id === d.id) open = false;
		});
	}

	function here() {
		open = false;
		if (cast.connected) void castDisconnect();
	}
</script>

{#snippet row(icon: typeof CastIcon, name: string, status: string, active: boolean, busy: boolean, onpick: () => void)}
	<button
		class="flex w-full items-center gap-3 rounded-lg px-2.5 py-2 text-left transition enabled:cursor-pointer disabled:opacity-50 {active
			? 'bg-primary/10'
			: 'enabled:hover:bg-foreground/10'}"
		disabled={!!cast.connecting && !busy}
		aria-current={active}
		onclick={onpick}
	>
		<span
			class="flex size-8 shrink-0 items-center justify-center rounded-md {active
				? 'bg-primary/15 text-primary'
				: 'bg-muted text-muted-foreground'}"
		>
			<HugeiconsIcon {icon} class="size-4" />
		</span>
		<span class="min-w-0 flex-1">
			<span class="block truncate text-sm font-medium {active ? 'text-primary' : ''}">{name}</span>
			<span class="block truncate text-xs text-muted-foreground">{status}</span>
		</span>
		{#if busy}
			<span class="size-4 shrink-0 animate-spin rounded-full border-2 border-current border-t-transparent text-muted-foreground"></span>
		{:else if active}
			<HugeiconsIcon icon={VolumeHighIcon} class="size-4 shrink-0 text-primary" />
		{/if}
	</button>
{/snippet}

<Popover.Root bind:open>
	<Popover.Trigger
		class="inline-flex size-8 cursor-pointer items-center justify-center rounded-md transition hover:bg-muted {cast.connected
			? 'text-primary'
			: 'text-muted-foreground hover:text-foreground'}"
		aria-label={label}
		title={label}
	>
		<HugeiconsIcon icon={CastIcon} class="h-5 w-5" />
	</Popover.Trigger>
	<Popover.Content side="top" align="end" sideOffset={10} class="w-80 max-w-[calc(100vw-2rem)] gap-0 overflow-hidden p-0">
		<div class="border-b px-4 pb-3 pt-3.5">
			<p class="font-heading text-sm font-semibold">{t('cast.title')}</p>
			<p class="mt-0.5 text-xs leading-relaxed text-muted-foreground">{t('cast.hint')}</p>
		</div>
		<div class="flex flex-col gap-0.5 p-1.5">
			{@render row(
				ComputerIcon,
				t('player.this_computer'),
				cast.connected ? t('cast.stop') : t('player.devices_this_computer_hint'),
				!cast.connected,
				false,
				here
			)}
			{#each cast.devices as d (d.id)}
				{@render row(
					isSpeaker(d) ? Speaker01Icon : Tv01Icon,
					d.name,
					cast.connecting === d.id
						? t('cast.connecting')
						: cast.connected?.id === d.id
							? t('cast.connected')
							: d.model,
					cast.connected?.id === d.id,
					cast.connecting === d.id,
					() => pick(d)
				)}
			{/each}
			{#if cast.devices.length === 0}
				<p class="flex items-center gap-2.5 px-2.5 py-3 text-xs leading-relaxed text-muted-foreground">
					{#if !slow}
						<span class="size-3.5 shrink-0 animate-spin rounded-full border-2 border-current border-t-transparent"></span>
					{/if}
					{slow ? t('cast.none_found') : t('cast.searching')}
				</p>
			{/if}
		</div>
		{#if cast.connected}
			<p class="border-t px-4 py-2.5 text-xs leading-relaxed text-muted-foreground">{t('cast.audio_only')}</p>
		{/if}
	</Popover.Content>
</Popover.Root>
