import Svg, { G, Line, Path, Polygon } from "react-native-svg";

type GroceryListIconProps = {
  size?: number;
};

const OUTLINE = "#4A6A3F";
const MUTED_GREEN = "#BFD9A7";
const LEAF_GREEN = "#7EA35A";
const SOFT_CREAM = "#F8FBF3";
const SOFT_OLIVE = "#A5AA65";
const SOFT_SAGE = "#B8D2C0";
const SOFT_WARM = "#E5D8AC";
const BASKET_FILL = "#F1E7BF";
const BASKET_ACCENT = "#D8C27E";

export function GroceryListIcon({ size = 66 }: GroceryListIconProps) {
  return (
    <Svg height={size} viewBox="0 0 66 66" width={size}>
      <Path
        d="M26.3,22.6c0.6,5.3,3.8,9.5,7.3,9.3c0.7,0,1.3-0.2,1.8-0.5c0.7-0.4,1.5-0.4,2.1,0c0.6,0.3,1.2,0.5,1.8,0.5 c3.5,0.2,6.8-4,7.3-9.3c0.6-5.3-1.8-9.9-5.3-10.1c-1.1-0.1-2.3,0.3-3.3,1.1c-1,0.7-2.3,0.7-3.3,0c-1-0.8-2.1-1.2-3.3-1.1 C28.1,12.8,25.7,17.3,26.3,22.6z"
        fill={MUTED_GREEN}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M36.5,14.2c0,0,1.9-3.5-1.5-6.3"
        fill="none"
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M36.8,9l0.4-0.8c1.4-2.4,4.5-3.2,6.9-1.8l0.8,0.4l-0.4,0.8c-1.4,2.4-4.5,3.2-6.9,1.8L36.8,9z"
        fill={LEAF_GREEN}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M41.2,37.5l1.1,0.6c2.6,1.4,5.8,0.4,7.1-2.2l13.4-25.5c1.5-2.8,0.4-6.3-2.4-7.8l-0.3-0.1 c-2.8-1.5-6.3-0.4-7.8,2.4L39,30.3C37.7,32.9,38.6,36.1,41.2,37.5z"
        fill={SOFT_WARM}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M55.3 11c-.7 1.3.7 3.3 2.9 4.5.5.3 1.1.5 1.6.6l2.2-4.2c-.4-.4-.9-.7-1.4-1C58.4 9.7 56 9.8 55.3 11zM51.2 18.9c-.7 1.3.7 3.3 2.9 4.5.5.3 1.1.5 1.6.6l2.2-4.2c-.4-.4-.9-.7-1.4-1C54.2 17.6 51.8 17.6 51.2 18.9z"
        fill={SOFT_CREAM}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M41.7,46c2.5,2,6.2,1.6,8.2-0.9c1.2-1.6,1.6-3.6,1-5.3c1.9-0.2,3.7-1.3,4.7-3.2c1-2,0.7-4.5-0.7-6.2 s-3.5-2.4-5.4-2.1c0.4-2-0.4-4.2-2.1-5.5c-1.8-1.5-4.3-1.6-6.3-0.6c-0.3-0.3-0.6-0.7-0.9-1c-2.5-2-6.2-1.6-8.2,0.9 c-2.5-2-6.2-1.6-8.2,0.9s-1.6,6.2,0.9,8.2"
        fill={SOFT_OLIVE}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M21.2,8.4V6.1h-7.5v2.3c0,0.9-0.3,1.8-0.8,2.6l-3.7,5.7c-1.1,1.7-1.7,3.7-1.7,5.8v23c0,1.4,1.2,2.6,2.6,2.6 h14.6c1.4,0,2.6-1.2,2.6-2.6v-23c0-2-0.6-4-1.7-5.8L22,11C21.5,10.3,21.2,9.3,21.2,8.4z"
        fill={SOFT_SAGE}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M21.6,6.1h-8.2c-1,0-1.9-0.9-1.9-1.9V3.7c0-1,0.9-1.9,1.9-1.9h8.2c1,0,1.9,0.9,1.9,1.9v0.4 C23.5,5.2,22.6,6.1,21.6,6.1z"
        fill={OUTLINE}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M24.5,14.9c-7-0.9-10.8,3.7-14,0l-1.2,1.8c-1.1,1.7-1.7,3.7-1.7,5.8v23c0,1.4,1.2,2.6,2.6,2.6h14.6 c1.4,0,2.6-1.2,2.6-2.6v-23c0-2-0.6-4-1.7-5.8L24.5,14.9z"
        fill={SOFT_CREAM}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M57.1,39.4H5.5c-1.7,0-3.1-1.4-3.1-3.1l0,0c0-1.7,1.4-3.1,3.1-3.1h51.7c1.7,0,3.1,1.4,3.1,3.1l0,0 C60.2,38,58.8,39.4,57.1,39.4z"
        fill={BASKET_FILL}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M47,64.2H15.6c-2.7,0-5-1.9-5.5-4.5l-4-20.3h50.5l-4.1,20.4C52,62.4,49.7,64.2,47,64.2z"
        fill={BASKET_FILL}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <G opacity={0.4}>
        <Path
          d="M12.6,56.7c0.2,1,0.3,2,0.7,3s1.4,1.6,2.4,1.6"
          fill="none"
          stroke="#FFFFFF"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeMiterlimit={10}
          strokeWidth={1.5}
        />
        <Line
          fill="none"
          stroke="#FFFFFF"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeMiterlimit={10}
          strokeWidth={1.5}
          x1="18.6"
          x2="19.8"
          y1="61.2"
          y2="61.2"
        />
      </G>
      <Polygon fill={BASKET_ACCENT} points="55.9 42.6 56.5 39.4 6.1 39.4 6.7 42.6" />
      <Path
        d="M47,64.2H15.6c-2.7,0-5-1.9-5.5-4.5l-4-20.3h50.5l-4.1,20.4C52,62.4,49.7,64.2,47,64.2z"
        fill="none"
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M21.2 58.6L21.2 58.6c-1.2 0-2.1-1-2.1-2.1v-9.2c0-1.2 1-2.1 2.1-2.1l0 0c1.2 0 2.1 1 2.1 2.1v9.2C23.3 57.6 22.4 58.6 21.2 58.6zM31.3 58.6L31.3 58.6c-1.2 0-2.1-1-2.1-2.1v-9.2c0-1.2 1-2.1 2.1-2.1l0 0c1.2 0 2.1 1 2.1 2.1v9.2C33.4 57.6 32.5 58.6 31.3 58.6zM41.4 58.6L41.4 58.6c-1.2 0-2.1-1-2.1-2.1v-9.2c0-1.2 1-2.1 2.1-2.1l0 0c1.2 0 2.1 1 2.1 2.1v9.2C43.5 57.6 42.6 58.6 41.4 58.6z"
        fill={SOFT_CREAM}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M58.5,58.5L58.5,58.5c-0.9,0.9-2.5,0.9-3.4,0L32,35.4c-0.9-0.9-0.9-2.5,0-3.4l0,0c0.9-0.9,2.5-0.9,3.4,0 l23.1,23C59.5,56,59.5,57.5,58.5,58.5z"
        fill={BASKET_FILL}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
      <Path
        d="M4.1,58.5L4.1,58.5c0.9,0.9,2.5,0.9,3.4,0l23.1-23.1c0.9-0.9,0.9-2.5,0-3.4l0,0c-0.9-0.9-2.5-0.9-3.4,0 L4.1,55C3.1,56,3.1,57.5,4.1,58.5z"
        fill={BASKET_FILL}
        stroke={OUTLINE}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeMiterlimit={10}
        strokeWidth={1.5}
      />
    </Svg>
  );
}
