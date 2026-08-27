# Mobile Assets

This folder contains checked-in assets used by the TableTogether mobile app.

The app must never depend on Desktop paths or temporary local files at runtime. Any image, SVG, Lottie animation or other visual asset used by React Native should live under `mobile/assets/`.

## Folder Map

- `brand/` - app icon and splash-related assets.
- `common/` - shared backgrounds, patterns and placeholders.
- `home/` - Home hero, Daily Food Tip illustrations and resource thumbnails.
- `navigation/` - custom bottom navigation icons.
- `meal_plan/` - meal-plan visuals, action icons and recipe-detail assets.
- `grocery/` - grocery category icons and grocery/package visuals.
- `insights/` - macro, chart and insights visuals.
- `household/` - account, household and profile-related visuals.

## Naming Rules

- Use lowercase snake_case file names.
- Keep names stable and descriptive.
- Avoid spaces, machine-specific names and temporary labels.
- Prefer names based on UI purpose, not on source location.

Good examples:

- `tip_fruit_choice.png`
- `highlight_practical_cooking.png`
- `grocery_vegetables.png`
- `nav_home.svg`
- `cooking_lottie.json`

## Format Rules

- Use PNG or WebP for illustrations and thumbnails.
- Use SVG/TSX components for simple single-color icons when they need theme colors.
- Use Lottie JSON only for intentional small animations.
- `lottie-react-native` is used for checked-in Lottie assets.
- Keep raster assets compressed and reasonably sized.
- Transparent backgrounds are preferred for icons and standalone illustrations.

## Runtime Rules

- Add the asset file before importing it in TypeScript.
- Do not import missing assets.
- Do not import optional files that are not checked in.
- Keep placeholders in code when a future asset is optional.
- Do not store source prompts, Desktop notes or temporary generation files here.

## Current Active Assets

- Home hero: `home/welcome/cooking_lottie.json`
- Daily Food Tips: `home/tips/tip_*.png`
- Page 1 resource thumbnails: `home/highlights/`, `home/family_kids/`, `home/healthy_habits/`
- Grocery category icons: `grocery/categories/`
- Bottom navigation icons: `navigation/`
- Additional brand or package assets should be added only when they are wired into the app.

The detailed per-folder placeholder README files were intentionally removed to keep this asset area easier to scan.
