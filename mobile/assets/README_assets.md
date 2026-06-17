# Mobile asset guide

This folder is the canonical place for mobile UI assets.

## Naming

- Use lowercase snake_case names.
- Avoid spaces, uppercase-only names and Romanian diacritics.
- Keep names descriptive and stable.
- Prefer paths that describe the UI area and usage.

Examples:
- `cooking_lottie.json`
- `cooking_loop.gif`
- `tip_fruit_choice.png`
- `tip_arranged_plate.png`
- `highlight_meal_prep.png`
- `kids_vegetables.png`
- `habit_grocery_planning.png`
- `grocery_scale.png`
- `grocery_bag.png`
- `nav_home.png`

## Formats

- Prefer PNG or WebP for raster UI images.
- Prefer Lottie JSON for small looped UI animations.
- GIF is acceptable as a fallback for small simple loops.
- Avoid MP4 unless the UI needs video playback.
- Avoid animated SVG unless the rendering approach is explicitly approved.

## Size rules

- Do not add large binary assets without a clear reason.
- Compress images before committing them.
- Transparent backgrounds are preferred for icons, hero loops and illustrations.
- Keep source assets larger than display size, but not excessive.

## Runtime rules

- Do not import missing assets in React Native code.
- Add assets first, then wire them into components.
- If an optional asset is missing, the UI must keep rendering a placeholder.
- `lottie-react-native` is now installed for the Home hero animation.
- Keep Lottie usage limited to real checked-in assets.

## Current state

UI-ASSETS-1 created the folder structure and documentation. The Home hero now has a real Lottie asset wired in through `mobile/assets/home/welcome/cooking_lottie.json`, and Daily Food Tip uses four local PNG illustrations from `mobile/assets/home/tips/`.
