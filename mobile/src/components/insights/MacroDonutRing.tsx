import { useEffect, useRef, useState } from "react";
import { Animated, Easing, StyleSheet, View } from "react-native";
import Svg, { Path } from "react-native-svg";

import { CaloriesFlameIcon } from "../icons/MacroNutrientIcons";

type MacroDonutRingProps = {
  carbsPercent: number;
  consumedCarbsRatio?: number;
  consumedFatRatio?: number;
  consumedProteinRatio?: number;
  fatPercent: number;
  proteinPercent: number;
  size?: number;
};

type DonutProgress = {
  carbs: number;
  fat: number;
  protein: number;
};

type DonutSegment = {
  baselineColor: string;
  color: string;
  percent: number;
  progress: number;
};

const PROTEIN_COLOR = "#EF3B45";
const CARBS_COLOR = "#F28A12";
const FAT_COLOR = "#F5C400";
const PROTEIN_BASELINE_COLOR = "#C96A70";
const CARBS_BASELINE_COLOR = "#C98A3A";
const FAT_BASELINE_COLOR = "#C6A63A";
const START_ANGLE = -106;
const SEGMENT_GAP_DEGREES = 4;

export function MacroDonutRing({
  carbsPercent,
  consumedCarbsRatio = 1,
  consumedFatRatio = 1,
  consumedProteinRatio = 1,
  fatPercent,
  proteinPercent,
  size = 180,
}: MacroDonutRingProps) {
  const targetProgress = {
    carbs: clampRatio(consumedCarbsRatio),
    fat: clampRatio(consumedFatRatio),
    protein: clampRatio(consumedProteinRatio),
  };
  const displayedProgressRef = useRef<DonutProgress>(targetProgress);
  const [displayedProgress, setDisplayedProgress] = useState<DonutProgress>(targetProgress);
  const center = size / 2;
  const ringWidth = Math.max(28, Math.round(size * 0.23));
  const outerRadius = center - Math.max(4, Math.round(size * 0.04));
  const innerRadius = Math.max(1, outerRadius - ringWidth);
  const cornerRadius = Math.max(3, Math.round(size * 0.035));
  const centerIconSize = Math.round(size * 0.48);
  const flameSize = Math.round(centerIconSize * 0.68);
  const segments = normalizeSegments([
    {
      baselineColor: PROTEIN_BASELINE_COLOR,
      color: PROTEIN_COLOR,
      percent: proteinPercent,
      progress: displayedProgress.protein,
    },
    {
      baselineColor: CARBS_BASELINE_COLOR,
      color: CARBS_COLOR,
      percent: carbsPercent,
      progress: displayedProgress.carbs,
    },
    {
      baselineColor: FAT_BASELINE_COLOR,
      color: FAT_COLOR,
      percent: fatPercent,
      progress: displayedProgress.fat,
    },
  ]);

  useEffect(() => {
    const animation = new Animated.Value(0);
    const fromProgress = displayedProgressRef.current;
    const listener = animation.addListener(({ value }) => {
      const nextProgress = interpolateProgress(fromProgress, targetProgress, value);
      displayedProgressRef.current = nextProgress;
      setDisplayedProgress(nextProgress);
    });

    Animated.timing(animation, {
      duration: 300,
      easing: Easing.out(Easing.cubic),
      toValue: 1,
      useNativeDriver: false,
    }).start(({ finished }) => {
      animation.removeListener(listener);
      if (finished) {
        displayedProgressRef.current = targetProgress;
        setDisplayedProgress(targetProgress);
      }
    });

    return () => {
      animation.stopAnimation();
      animation.removeListener(listener);
    };
  }, [targetProgress.carbs, targetProgress.fat, targetProgress.protein]);

  return (
    <View style={[styles.shell, { height: size, width: size }]}>
      <Svg height={size} style={StyleSheet.absoluteFill} width={size}>
        {renderRingSegments(segments, center, innerRadius, outerRadius, cornerRadius, "baseline")}
        {renderRingSegments(segments, center, innerRadius, outerRadius, cornerRadius, "consumed")}
      </Svg>
      <View
        style={[
          styles.centerIcon,
          {
            borderRadius: centerIconSize / 2,
            height: centerIconSize,
            width: centerIconSize,
          },
        ]}
      >
        <CaloriesFlameIcon size={flameSize} />
      </View>
    </View>
  );
}

function renderRingSegments(
  segments: DonutSegment[],
  center: number,
  innerRadius: number,
  outerRadius: number,
  cornerRadius: number,
  layer: "baseline" | "consumed",
) {
  if (segments.length === 0) {
    return null;
  }

  if (segments.length === 1) {
    const segment = segments[0];
    const progress = layer === "baseline" ? 1 : segment.progress;
    if (progress <= 0.005) {
      return null;
    }
    if (progress >= 0.995) {
      return (
        <Path
          d={fullRingPath(center, center, innerRadius, outerRadius)}
          fill={layer === "baseline" ? segment.baselineColor : segment.color}
          fillRule="evenodd"
        />
      );
    }
    return (
      <Path
        d={roundedAnnularSectorPath(
          center,
          center,
          innerRadius,
          outerRadius,
          START_ANGLE,
          START_ANGLE + 360 * progress,
          cornerRadius,
        )}
        fill={layer === "baseline" ? segment.baselineColor : segment.color}
      />
    );
  }

  let cursor = START_ANGLE;
  const availableDegrees = 360 - SEGMENT_GAP_DEGREES * segments.length;

  return segments.map((segment, index) => {
    const span = (segment.percent / 100) * availableDegrees;
    const start = cursor;
    const segmentEnd = cursor + span;
    const progress = layer === "baseline" ? 1 : segment.progress;
    const end = start + span * progress;
    cursor = segmentEnd + SEGMENT_GAP_DEGREES;

    if (layer === "consumed" && progress <= 0.005) {
      return null;
    }

    return (
      <Path
        d={roundedAnnularSectorPath(
          center,
          center,
          innerRadius,
          outerRadius,
          start,
          end,
          cornerRadius,
        )}
        fill={layer === "baseline" ? segment.baselineColor : segment.color}
        key={`macro-sector-${layer}-${segment.color}-${index}`}
      />
    );
  });
}

function normalizeSegments(segments: DonutSegment[]) {
  const safeSegments = segments
    .map((segment) => ({
      ...segment,
      percent: Number.isFinite(segment.percent) ? Math.max(0, segment.percent) : 0,
      progress: clampRatio(segment.progress),
    }))
    .filter((segment) => segment.percent > 0.2);
  const total = safeSegments.reduce((sum, segment) => sum + segment.percent, 0);
  if (total <= 0) {
    return [];
  }
  return safeSegments.map((segment) => ({
    ...segment,
    percent: (segment.percent / total) * 100,
  }));
}

function interpolateProgress(
  fromProgress: DonutProgress,
  toProgress: DonutProgress,
  value: number,
): DonutProgress {
  return {
    carbs: interpolateNumber(fromProgress.carbs, toProgress.carbs, value),
    fat: interpolateNumber(fromProgress.fat, toProgress.fat, value),
    protein: interpolateNumber(fromProgress.protein, toProgress.protein, value),
  };
}

function interpolateNumber(start: number, end: number, value: number): number {
  return start + (end - start) * value;
}

function clampRatio(value: number): number {
  return Number.isFinite(value) ? Math.min(1, Math.max(0, value)) : 0;
}

function roundedAnnularSectorPath(
  centerX: number,
  centerY: number,
  innerRadius: number,
  outerRadius: number,
  startAngle: number,
  endAngle: number,
  cornerRadius: number,
) {
  const span = Math.max(0, endAngle - startAngle);
  const radialRadius = Math.min(cornerRadius, (outerRadius - innerRadius) / 2 - 0.5);
  const maxAngleOffset = Math.max(0, span / 3);
  const outerAngleOffset = Math.min(radiansToDegrees(radialRadius / outerRadius), maxAngleOffset);
  const innerAngleOffset = Math.min(radiansToDegrees(radialRadius / innerRadius), maxAngleOffset);
  const outerStart = polarToCartesian(centerX, centerY, outerRadius, startAngle + outerAngleOffset);
  const outerEnd = polarToCartesian(centerX, centerY, outerRadius, endAngle - outerAngleOffset);
  const outerEndCorner = polarToCartesian(centerX, centerY, outerRadius, endAngle);
  const outerEndSide = polarToCartesian(centerX, centerY, outerRadius - radialRadius, endAngle);
  const innerEndSide = polarToCartesian(centerX, centerY, innerRadius + radialRadius, endAngle);
  const innerEndCorner = polarToCartesian(centerX, centerY, innerRadius, endAngle);
  const innerEnd = polarToCartesian(centerX, centerY, innerRadius, endAngle - innerAngleOffset);
  const innerStart = polarToCartesian(centerX, centerY, innerRadius, startAngle + innerAngleOffset);
  const innerStartCorner = polarToCartesian(centerX, centerY, innerRadius, startAngle);
  const innerStartSide = polarToCartesian(centerX, centerY, innerRadius + radialRadius, startAngle);
  const outerStartSide = polarToCartesian(centerX, centerY, outerRadius - radialRadius, startAngle);
  const outerStartCorner = polarToCartesian(centerX, centerY, outerRadius, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";

  return [
    `M ${outerStart.x} ${outerStart.y}`,
    `A ${outerRadius} ${outerRadius} 0 ${largeArcFlag} 1 ${outerEnd.x} ${outerEnd.y}`,
    `Q ${outerEndCorner.x} ${outerEndCorner.y} ${outerEndSide.x} ${outerEndSide.y}`,
    `L ${innerEndSide.x} ${innerEndSide.y}`,
    `Q ${innerEndCorner.x} ${innerEndCorner.y} ${innerEnd.x} ${innerEnd.y}`,
    `A ${innerRadius} ${innerRadius} 0 ${largeArcFlag} 0 ${innerStart.x} ${innerStart.y}`,
    `Q ${innerStartCorner.x} ${innerStartCorner.y} ${innerStartSide.x} ${innerStartSide.y}`,
    `L ${outerStartSide.x} ${outerStartSide.y}`,
    `Q ${outerStartCorner.x} ${outerStartCorner.y} ${outerStart.x} ${outerStart.y}`,
    "Z",
  ].join(" ");
}

function radiansToDegrees(value: number): number {
  return (value * 180) / Math.PI;
}

function fullRingPath(
  centerX: number,
  centerY: number,
  innerRadius: number,
  outerRadius: number,
) {
  return [
    `M ${centerX} ${centerY - outerRadius}`,
    `A ${outerRadius} ${outerRadius} 0 1 1 ${centerX} ${centerY + outerRadius}`,
    `A ${outerRadius} ${outerRadius} 0 1 1 ${centerX} ${centerY - outerRadius}`,
    `M ${centerX} ${centerY - innerRadius}`,
    `A ${innerRadius} ${innerRadius} 0 1 0 ${centerX} ${centerY + innerRadius}`,
    `A ${innerRadius} ${innerRadius} 0 1 0 ${centerX} ${centerY - innerRadius}`,
    "Z",
  ].join(" ");
}

function polarToCartesian(centerX: number, centerY: number, radius: number, angle: number) {
  const radians = ((angle - 90) * Math.PI) / 180;
  return {
    x: centerX + radius * Math.cos(radians),
    y: centerY + radius * Math.sin(radians),
  };
}

const styles = StyleSheet.create({
  centerIcon: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    justifyContent: "center",
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.06,
    shadowRadius: 8,
  },
  shell: {
    alignItems: "center",
    justifyContent: "center",
  },
});
