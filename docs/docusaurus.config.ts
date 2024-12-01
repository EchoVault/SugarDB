// @ts-ignore
import { themes as prismThemes } from "prism-react-renderer";
// @ts-ignore
import type { Config } from "@docusaurus/types";
// @ts-ignore
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "SugarDB",
  tagline: "Embeddable distributed in-memory data store.",
  favicon: "img/logo.png",

  // Set the production url of your site here
  url: "https://sugardb.io",
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: "/",

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: "", // Usually your GitHub org/user name.
  projectName: "SugarDB", // Usually your repo name.

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  // Custom plugin for hot reloading
  plugins: [
    function hotReload() {
      return {
        name: "hot-reload",
        configureWebpack() {
          return {
            watchOptions: {
              poll: 1000, // Check for changes every second
              aggregateTimeout: 300, // Delay before rebuilding
            },
          };
        },
      };
    },
  ],

  presets: [
    [
      "classic",
      {
        docs: {
          sidebarPath: "./sidebars.ts",
        },
        blog: {
          showReadingTime: true,
        },
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    algolia: {
      appId: "QGK73FSNRI",
      apiKey: "f9225d8721591a9664e4346847407e2d",
      indexName: "echovault",
      contextualSearch: false,
    },
    // Replace with your project's social card
    navbar: {
      title: "",
      style: "dark",
      logo: {
        alt: "SugarDB Logo",
        src: "img/logo.png",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "documentationSidebar",
          position: "right",
          label: "Documentation",
        },
        {
          href: "https://github.com/EchoVault/SugarDB",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            {
              label: "Documentation",
              to: "/docs/intro",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "Discord",
              href: "https://discord.gg/JrG4kPrF8v",
            },
          ],
        },
        {
          title: "More",
          items: [
            {
              label: "GitHub",
              href: "https://github.com/EchoVault/SugarDB",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} SugarDB.`,
    },
    prism: {
      additionalLanguages: ["lua"],
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
