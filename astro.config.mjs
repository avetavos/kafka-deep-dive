// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

import preact from '@astrojs/preact';

// https://astro.build/config
export default defineConfig({
  site: 'https://deep-dive.avetavos.com',
  base: '/kafka',
  output: 'static',
  integrations: [starlight({
      title: 'Kafka — From Zero to Hero',
      head: [
        { tag: 'script', attrs: { type: 'module', src: '/kafka/enhance.js' } },
        { tag: 'link', attrs: { rel: 'manifest', href: '/kafka/manifest.webmanifest' } },
        { tag: 'link', attrs: { rel: 'apple-touch-icon', href: '/kafka/apple-touch-icon.png' } },
        { tag: 'link', attrs: { rel: 'icon', type: 'image/png', sizes: '192x192', href: '/kafka/icon-192.png' } },
        { tag: 'meta', attrs: { name: 'theme-color', content: '#00A88F' } },
        { tag: 'meta', attrs: { name: 'mobile-web-app-capable', content: 'yes' } },
        { tag: 'meta', attrs: { name: 'apple-mobile-web-app-capable', content: 'yes' } },
        { tag: 'meta', attrs: { name: 'apple-mobile-web-app-status-bar-style', content: 'black-translucent' } },
        { tag: 'meta', attrs: { name: 'apple-mobile-web-app-title', content: "Kafka — From Zero to Hero" } },
        { tag: 'script', content: "if('serviceWorker' in navigator){window.addEventListener('load',function(){navigator.serviceWorker.register('/kafka/sw.js',{scope:'/kafka/'}).catch(function(){})})}" },
      ],
      defaultLocale: 'en',
      locales: {
        en: { label: 'English', lang: 'en' },
        th: { label: 'ไทย', lang: 'th' },
      },
      customCss: ['./src/styles/custom.css'],
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/avetavos/kafka-deep-dive' }],
      sidebar: [
        { label: 'Kafka Foundations & the Log', items: [{ autogenerate: { directory: 'foundations' } }] },
        { label: 'Topics, Partitions & Replication', items: [{ autogenerate: { directory: 'topics-and-partitions' } }] },
        { label: 'Producing Messages', items: [{ autogenerate: { directory: 'producers' } }] },
        { label: 'Consuming & Consumer Groups', items: [{ autogenerate: { directory: 'consumers' } }] },
        { label: 'Delivery Semantics & Transactions', items: [{ autogenerate: { directory: 'delivery-and-transactions' } }] },
        { label: 'Streams, Connect & Schema Registry', items: [{ autogenerate: { directory: 'streams-connect-schema' } }] },
        { label: 'Operations & Production', items: [{ autogenerate: { directory: 'operations-and-production' } }] },
      ],
      }), preact()],
});
