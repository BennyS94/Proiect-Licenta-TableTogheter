import Svg, { Circle, Ellipse, Path, Rect } from "react-native-svg";

type MacroNutrientIconProps = {
  size?: number;
};

export function CaloriesFlameIcon({ size = 32 }: MacroNutrientIconProps) {
  return (
    <Svg height={size} viewBox="0 0 64 64" width={size}>
      <Path
        d="M34 7c6.8 7.2 12.8 14.2 11.7 24.2 3-2.1 4.7-5 5.2-8.8 7.2 9.3 5.9 23.8-3.8 31.1-9.1 6.9-24 5.6-31.3-3.8C7.2 38.5 12 24 25.8 14.6c4.1-2.8 6.6-5.3 8.2-7.6Z"
        fill="#1F252D"
      />
      <Path
        d="M33.2 36.5c4.3 4.3 7.9 8.3 7.5 13.1-.5 5.7-5 8.9-9.6 8.9-5.3 0-9.8-3.9-9.8-9.4 0-5.3 3.8-9 8.5-12.5 1.3-1 2.5-2 3.4-3.2.3 1 .3 2 .1 3.1Z"
        fill="#FFFFFF"
      />
    </Svg>
  );
}

export function ProteinDrumstickIcon({ size = 32 }: MacroNutrientIconProps) {
  return (
    <Svg height={size} viewBox="0 0 64 64" width={size}>
      <Path
        d="M38.9 7.5c10.6 0 18.6 7.8 18.6 18 0 11-9.2 20.4-21.2 20.4-3 0-5.7-.6-8.2-1.8L18 54.2c-2.6 2.6-6.9 2.6-9.5 0s-2.6-6.9 0-9.5l10.1-10.1c-1.2-2.6-1.5-5.7-.9-8.9C19.4 15.3 28 7.5 38.9 7.5Z"
        fill="#F0444E"
      />
      <Path
        d="M26.5 41.5 13.4 54.6"
        fill="none"
        stroke="#F0444E"
        strokeLinecap="round"
        strokeWidth="10"
      />
      <Circle cx="9.8" cy="52.9" fill="#F0444E" r="6.7" />
      <Circle cx="17.1" cy="59" fill="#F0444E" r="5.7" />
      <Path
        d="M41.8 14.2c5.2 1 8.8 4.8 9.6 9.9"
        fill="none"
        stroke="#FFFFFF"
        strokeLinecap="round"
        strokeWidth="5.4"
      />
      <Path
        d="M50.7 33.5c-2.7 4.6-7.9 7.2-14.1 6.9"
        fill="none"
        opacity="0.2"
        stroke="#9F1F2B"
        strokeLinecap="round"
        strokeWidth="4.4"
      />
    </Svg>
  );
}

export function CarbsWheatIcon({ size = 32 }: MacroNutrientIconProps) {
  return (
    <Svg height={size} viewBox="0 0 64 64" width={size}>
      <Rect fill="#F28A12" height="34" rx="4" width="8" x="28" y="24" />
      <Path
        d="M32 5c7.7 4.9 9.8 11.8 0 18.7C22.2 16.8 24.3 9.9 32 5Z"
        fill="#F28A12"
      />
      <Path
        d="M19 19.2c9.6.9 15 6.6 13.2 17-10.4-.6-15.8-6.5-13.2-17Z"
        fill="#F28A12"
      />
      <Path
        d="M45 19.2c-9.6.9-15 6.6-13.2 17 10.4-.6 15.8-6.5 13.2-17Z"
        fill="#F28A12"
      />
      <Path
        d="M18.5 37.3c9 .5 14.1 5.2 13.1 15-9.8-.4-15.1-5.1-13.1-15Z"
        fill="#F28A12"
      />
      <Path
        d="M45.5 37.3c-9 .5-14.1 5.2-13.1 15 9.8-.4 15.1-5.1 13.1-15Z"
        fill="#F28A12"
      />
    </Svg>
  );
}

export function FatsAvocadoIcon({ size = 32 }: MacroNutrientIconProps) {
  return (
    <Svg height={size} viewBox="0 0 64 64" width={size}>
      <Path
        d="M32 6.7c-7.9 0-13.5 8.1-17.8 17.8-4.7 10.5-6.2 23.3 4 29.8 8.4 5.3 21.2 5.3 29.6 0 10.2-6.5 8.7-19.3 4-29.8C47.5 14.8 39.9 6.7 32 6.7Z"
        fill="none"
        stroke="#F5C400"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="6.5"
      />
      <Circle cx="32" cy="36.5" fill="#F5C400" r="11" />
    </Svg>
  );
}
