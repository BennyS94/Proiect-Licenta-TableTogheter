import type { ReactElement } from "react";
import Svg, { Path } from "react-native-svg";

import { colors } from "../../../theme/colors";
import {
  INGREDIENT_GENERIC_PATH,
  INGREDIENT_MEASURING_JUG_PATH,
  INGREDIENT_MEASURING_SPOONS_PATH,
  INGREDIENT_PIECES_PATH,
  INGREDIENT_PRODUCE_PATH,
  INGREDIENT_SCALE_PATH,
  INGREDIENT_SEASONING_PATH,
} from "./ingredientMeasurementIconPaths";

export type IngredientMeasurementIconKind =
  | "measuring_jug"
  | "measuring_spoons"
  | "scale"
  | "pieces"
  | "produce"
  | "seasoning"
  | "generic";

export type IngredientMeasurementIconProps = {
  color?: string;
  size?: number;
  strokeWidth?: number;
};

type IngredientIconSelectionInput = {
  ingredientName?: string | null;
  quantityUnit?: string | null;
  rawText?: string | null;
};

type IngredientIconComponent = (props: IngredientMeasurementIconProps) => ReactElement;

const DEFAULT_ICON_COLOR = colors.accentDark;
const DEFAULT_ICON_CUTOUT_COLOR = "#EEF7E8";
const MEASURING_SPOONS_SMALL_BOWL_CUTOUT_PATH =
  "M86.5 91.7 C86.5 86.9 90.3 83.5 94.7 84.4 C99.1 85.3 101.4 89.8 99.6 94.2 C97.9 98.5 92.1 100.1 88.4 97.3 C87.1 96.2 86.5 94.1 86.5 91.7 Z " +
  "M104.5 78.6 C104.5 74.4 108.1 71.7 112.1 72.5 C116 73.3 118.3 76.8 117.2 80.6 C116.1 84.5 111.4 86.3 107.9 84.1 C105.7 82.8 104.5 80.8 104.5 78.6 Z";

const UNIT_KIND_MAP: Record<string, IngredientMeasurementIconKind> = {
  cup: "measuring_jug",
  cups: "measuring_jug",
  l: "measuring_jug",
  liter: "measuring_jug",
  liters: "measuring_jug",
  ml: "measuring_jug",
  tablespoon: "measuring_spoons",
  tablespoons: "measuring_spoons",
  tbsp: "measuring_spoons",
  teaspoon: "measuring_spoons",
  teaspoons: "measuring_spoons",
  tsp: "measuring_spoons",
  g: "scale",
  gram: "scale",
  grams: "scale",
  kg: "scale",
  kilogram: "scale",
  kilograms: "scale",
  lb: "scale",
  lbs: "scale",
  ounce: "scale",
  ounces: "scale",
  oz: "scale",
  clove: "pieces",
  cloves: "pieces",
  count: "pieces",
  cube: "pieces",
  cubes: "pieces",
  egg: "pieces",
  eggs: "pieces",
  fillet: "pieces",
  fillets: "pieces",
  piece: "pieces",
  pieces: "pieces",
  slice: "pieces",
  slices: "pieces",
  strip: "pieces",
  strips: "pieces",
  bunch: "produce",
  bunches: "produce",
  head: "produce",
  heads: "produce",
  sprig: "produce",
  sprigs: "produce",
  stalk: "produce",
  stalks: "produce",
  dash: "seasoning",
  dashes: "seasoning",
  pinch: "seasoning",
  pinches: "seasoning",
};

const TEXT_RULES: Array<{
  kind: IngredientMeasurementIconKind;
  patterns: RegExp[];
}> = [
  {
    kind: "measuring_jug",
    patterns: [
      /\b(cup|cups|ml|l|liter|liters)\b/,
      /\b(liquid|broth|stock|water|milk|juice)\b/,
    ],
  },
  {
    kind: "measuring_spoons",
    patterns: [/\b(teaspoon|teaspoons|tsp|tablespoon|tablespoons|tbsp)\b/],
  },
  {
    kind: "scale",
    patterns: [
      /\b(pound|pounds|lb|lbs|ounce|ounces|oz|gram|grams|kg|kilogram|kilograms)\b/,
      /(^|\s|\d)g\b/,
    ],
  },
  {
    kind: "pieces",
    patterns: [
      /\b(egg|eggs|egg white|egg whites|clove|cloves|cube|cubes)\b/,
      /\b(slice|slices|strip|strips|fillet|fillets|piece|pieces)\b/,
    ],
  },
  {
    kind: "produce",
    patterns: [
      /\b(head|heads|bunch|bunches|stalk|stalks|sprig|sprigs)\b/,
      /\b(onion|onions|green onion|red onion|potato|potatoes)\b/,
      /\b(bell pepper|bell peppers|carrot|carrots|mushroom|mushrooms)\b/,
      /\b(eggplant|zucchini|lemon|lemon wedges|fresh ginger)\b/,
    ],
  },
  {
    kind: "seasoning",
    patterns: [
      /\b(pinch|pinches|dash|dashes)\b/,
      /\b(salt|pepper|black pepper|spices|seasoning|herbs)\b/,
    ],
  },
];

const ICON_COMPONENTS: Record<IngredientMeasurementIconKind, IngredientIconComponent> = {
  generic: IngredientGenericIcon,
  measuring_jug: IngredientMeasuringJugIcon,
  measuring_spoons: IngredientMeasuringSpoonsIcon,
  pieces: IngredientPiecesIcon,
  produce: IngredientProduceIcon,
  scale: IngredientScaleIcon,
  seasoning: IngredientSeasoningIcon,
};

export function IngredientMeasurementIcon({
  kind,
  ...props
}: IngredientMeasurementIconProps & { kind: IngredientMeasurementIconKind }) {
  const Icon = ICON_COMPONENTS[kind] ?? IngredientGenericIcon;
  return <Icon {...props} />;
}

export function getIngredientMeasurementIconKind({
  ingredientName,
  quantityUnit,
  rawText,
}: IngredientIconSelectionInput): IngredientMeasurementIconKind {
  const combinedText = [rawText, ingredientName].filter(Boolean).join(" ");
  const primaryText = normalizeIconText(combinedText.replace(/\([^)]*\)/g, " "));
  if (isSeasoningText(primaryText)) {
    return "seasoning";
  }

  const unitKind = unitToIconKind(quantityUnit);
  if (unitKind) {
    return unitKind;
  }

  const primaryKind = matchTextRule(primaryText);
  if (primaryKind) {
    return primaryKind;
  }

  const fullText = normalizeIconText(combinedText);
  if (!fullText) {
    return "generic";
  }

  return matchTextRule(fullText) ?? "generic";
}

function matchTextRule(text: string): IngredientMeasurementIconKind | null {
  for (const rule of TEXT_RULES) {
    if (rule.patterns.some((pattern) => pattern.test(text))) {
      return rule.kind;
    }
  }

  return null;
}

function isSeasoningText(text: string): boolean {
  return /\b(salt|pepper|black pepper|spices|seasoning|herbs)\b/.test(text);
}

export function IngredientMeasuringJugIcon({
  color = DEFAULT_ICON_COLOR,
  size = 18,
}: IngredientMeasurementIconProps) {
  return <IngredientPathIcon color={color} path={INGREDIENT_MEASURING_JUG_PATH} size={size} />;
}

export function IngredientMeasuringSpoonsIcon({
  color = DEFAULT_ICON_COLOR,
  size = 18,
}: IngredientMeasurementIconProps) {
  return (
    <Svg height={size} viewBox="0 0 128 128" width={size}>
      <Path d={INGREDIENT_MEASURING_SPOONS_PATH} fill={color} fillRule="evenodd" />
      <Path
        d={MEASURING_SPOONS_SMALL_BOWL_CUTOUT_PATH}
        fill={DEFAULT_ICON_CUTOUT_COLOR}
        stroke={color}
        strokeLinejoin="round"
        strokeWidth={2.5}
      />
    </Svg>
  );
}

export function IngredientScaleIcon({
  color = DEFAULT_ICON_COLOR,
  size = 18,
}: IngredientMeasurementIconProps) {
  return <IngredientPathIcon color={color} path={INGREDIENT_SCALE_PATH} size={size} />;
}

export function IngredientPiecesIcon({
  color = DEFAULT_ICON_COLOR,
  size = 18,
}: IngredientMeasurementIconProps) {
  return <IngredientPathIcon color={color} path={INGREDIENT_PIECES_PATH} size={size} />;
}

export function IngredientProduceIcon({
  color = DEFAULT_ICON_COLOR,
  size = 18,
}: IngredientMeasurementIconProps) {
  return <IngredientPathIcon color={color} path={INGREDIENT_PRODUCE_PATH} size={size} />;
}

export function IngredientSeasoningIcon({
  color = DEFAULT_ICON_COLOR,
  size = 18,
}: IngredientMeasurementIconProps) {
  return <IngredientPathIcon color={color} path={INGREDIENT_SEASONING_PATH} size={size} />;
}

export function IngredientGenericIcon({
  color = DEFAULT_ICON_COLOR,
  size = 18,
}: IngredientMeasurementIconProps) {
  return <IngredientPathIcon color={color} path={INGREDIENT_GENERIC_PATH} size={size} />;
}

function IngredientPathIcon({
  color,
  path,
  size,
}: {
  color: string;
  path: string;
  size: number;
}) {
  return (
    <Svg height={size} viewBox="0 0 128 128" width={size}>
      <Path d={path} fill={color} fillRule="evenodd" />
    </Svg>
  );
}

function unitToIconKind(unit: string | null | undefined): IngredientMeasurementIconKind | null {
  const normalizedUnit = normalizeIconText(unit ?? "");
  if (!normalizedUnit) {
    return null;
  }
  return UNIT_KIND_MAP[normalizedUnit] ?? null;
}

function normalizeIconText(value: string): string {
  return value
    .toLowerCase()
    .replace(/[()]/g, " ")
    .replace(/[_/-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
