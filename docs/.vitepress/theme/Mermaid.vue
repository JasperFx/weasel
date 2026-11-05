<script setup lang="ts">
// Renders one mermaid diagram in the browser. This is the small slice of vitepress-plugin-mermaid
// we actually used, owned here so the docs build does not depend on a plugin whose peer range stops
// at mermaid 11 (weasel#729). mermaid is imported lazily so it never runs during SSR.
import { onMounted, ref } from 'vue'

const props = defineProps<{ code: string }>()
const svg = ref('')
const failed = ref(false)
const id = `mermaid-${Math.random().toString(36).slice(2)}`

onMounted(async () => {
  try {
    const mermaid = (await import('mermaid')).default
    const dark = document.documentElement.classList.contains('dark')
    mermaid.initialize({ startOnLoad: false, theme: dark ? 'dark' : 'default' })
    svg.value = (await mermaid.render(id, decodeURIComponent(props.code))).svg
  } catch (e) {
    console.error(e)
    failed.value = true
  }
})
</script>

<template>
  <div v-if="svg" class="mermaid" v-html="svg" />
  <pre v-else-if="failed"><code>{{ decodeURIComponent(code) }}</code></pre>
  <div v-else class="mermaid" />
</template>
