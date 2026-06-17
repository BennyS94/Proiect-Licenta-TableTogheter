import type { ComponentType } from "react";
import { Pressable, StyleSheet, View } from "react-native";

import {
  HomeNavIcon,
  HouseholdNavIcon,
  InsightsNavIcon,
  MealPlanNavIcon,
} from "../icons/NavigationIcons";
import { colors } from "../../theme/colors";

export type AppPageKey = "home" | "mealPlan" | "insights" | "household";

export const FLOATING_NAV_BOTTOM_OFFSET = 18;
export const FLOATING_NAV_CONTENT_GAP = 22;
export const FLOATING_NAV_HEIGHT = 60;
export const FLOATING_NAV_SCREEN_BOTTOM_PADDING =
  FLOATING_NAV_HEIGHT + FLOATING_NAV_BOTTOM_OFFSET + FLOATING_NAV_CONTENT_GAP;

type FloatingNavProps = {
  activePage: AppPageKey;
  onSelectPage: (page: AppPageKey) => void;
};

const ITEMS: Array<{
  Icon: ComponentType<{ color?: string; size?: number }>;
  key: AppPageKey;
  label: string;
}> = [
  { Icon: HomeNavIcon, key: "home", label: "Home" },
  { Icon: MealPlanNavIcon, key: "mealPlan", label: "Meal Plan" },
  { Icon: InsightsNavIcon, key: "insights", label: "Insights" },
  { Icon: HouseholdNavIcon, key: "household", label: "Household and Account" },
];

export function FloatingNav({ activePage, onSelectPage }: FloatingNavProps) {
  return (
    <View style={styles.shell} pointerEvents="box-none">
      <View style={styles.bar}>
        {ITEMS.map((item) => {
          const active = item.key === activePage;
          const Icon = item.Icon;
          return (
            <Pressable
              accessibilityLabel={item.label}
              accessibilityRole="button"
              key={item.key}
              onPress={() => onSelectPage(item.key)}
              style={({ pressed }) => [
                styles.item,
                active ? styles.itemActive : null,
                pressed ? styles.pressed : null,
              ]}
            >
              <Icon color={active ? "#FFFFFF" : "#4B5563"} size={26} />
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  bar: {
    alignItems: "center",
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 26,
    borderWidth: 1,
    flexDirection: "row",
    gap: 8,
    height: FLOATING_NAV_HEIGHT,
    justifyContent: "space-between",
    padding: 8,
    shadowColor: "#000000",
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.16,
    shadowRadius: 16,
  },
  item: {
    alignItems: "center",
    borderRadius: 20,
    height: 42,
    justifyContent: "center",
    width: 54,
  },
  itemActive: {
    backgroundColor: colors.accent,
  },
  pressed: {
    opacity: 0.82,
  },
  shell: {
    bottom: FLOATING_NAV_BOTTOM_OFFSET,
    left: 18,
    position: "absolute",
    right: 18,
  },
});
