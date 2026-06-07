import { useMemo, useState } from "react";
import { Linking, Pressable, StyleSheet, Text, useWindowDimensions, View } from "react-native";
import LottieView from "lottie-react-native";

import { ResourceCarouselSection } from "../components/home/ResourceCarouselSection";
import { TipLightbulbLeafIcon } from "../components/icons/TipLightbulbLeafIcon";
import { AppScreen } from "../components/ui/AppScreen";
import {
  dailyFoodTips,
  familyKidsIdeas,
  healthyHabits,
  weeklyHighlights,
  type HomeResourceItem,
} from "../data/homeContent";

const cookingLottie = require("../../assets/home/welcome/cooking_lottie.json");

type HomePageProps = {
  activeProfileName: string;
  householdName: string;
  isSetupComplete: boolean;
  onGoToHousehold: () => void;
  onGoToMealPlan: () => void;
  profileCount: number;
};

type HomeSubPage = "highlights" | "familyKids" | "healthyHabits";

const HOME_SECTIONS: Record<
  HomeSubPage,
  {
    items: HomeResourceItem[];
    title: string;
  }
> = {
  familyKids: {
    items: familyKidsIdeas,
    title: "Family & Kids Food Ideas",
  },
  healthyHabits: {
    items: healthyHabits,
    title: "Healthy Habits",
  },
  highlights: {
    items: weeklyHighlights,
    title: "Highlights of the Week",
  },
};

export function HomePage({
  activeProfileName,
  householdName,
  isSetupComplete,
  onGoToMealPlan,
  profileCount,
}: HomePageProps) {
  const { width } = useWindowDimensions();
  const [tipIndex, setTipIndex] = useState(0);
  const [homeSubPage, setHomeSubPage] = useState<HomeSubPage | null>(null);

  const contentWidth = Math.max(280, width - 40);
  const hasProfiles = isSetupComplete && profileCount > 0;
  const profileName = hasProfiles && activeProfileName.trim() ? activeProfileName.trim() : "there";
  const safeHouseholdName =
    isSetupComplete && householdName.trim() ? householdName.trim() : "Your Household";
  const currentTip = dailyFoodTips[tipIndex % dailyFoodTips.length] ?? dailyFoodTips[0];
  const animationWidth = Math.min(194, Math.max(174, Math.round(contentWidth * 0.52)));
  const animationHeight = Math.min(164, Math.max(150, Math.round(animationWidth * 0.84)));

  const profileCountLabel = useMemo(() => {
    if (profileCount === 1) {
      return "1 profile configured";
    }
    return `${profileCount} profiles configured`;
  }, [profileCount]);

  if (homeSubPage) {
    const section = HOME_SECTIONS[homeSubPage];
    return (
      <HomeSeeAllPage
        contentWidth={contentWidth}
        items={section.items}
        onBack={() => setHomeSubPage(null)}
        title={section.title}
      />
    );
  }

  return (
    <AppScreen contentContainerStyle={styles.screen}>
      <View style={[styles.hero, { minHeight: animationHeight, width: contentWidth }]}>
        <View style={styles.heroCopy}>
          <Text
            adjustsFontSizeToFit
            minimumFontScale={0.72}
            numberOfLines={1}
            style={styles.greeting}
          >
            Good evening,
          </Text>
          <Text
            adjustsFontSizeToFit
            minimumFontScale={0.7}
            numberOfLines={1}
            style={styles.profileName}
          >
            {profileName} 👋
          </Text>
          <Text style={styles.readyText}>Ready to cook?</Text>
        </View>
        <View style={[styles.animationCard, { height: animationHeight, width: animationWidth }]}>
          <LottieView
            autoPlay
            loop
            resizeMode="contain"
            source={cookingLottie}
            speed={0.9}
            style={styles.heroLottie}
          />
        </View>
      </View>

      <View style={[styles.householdCard, { width: contentWidth }]}>
        <View style={styles.householdTopRow}>
          <View style={styles.householdIconCircle}>
            <Text style={styles.householdIcon}>⌂</Text>
          </View>
          <View style={styles.householdCopy}>
            <Text numberOfLines={1} style={styles.householdTitle}>
              {hasProfiles ? safeHouseholdName : "Set up your household"}
            </Text>
            <Text style={styles.householdMeta}>
              {hasProfiles ? profileCountLabel : "0 profiles configured"}
            </Text>
            <Text numberOfLines={2} style={styles.householdBody}>
              {hasProfiles
                ? "Ready to plan balanced meals for your household."
                : "Add member profiles to start planning balanced meals."}
            </Text>
          </View>
        </View>
        <Pressable
          accessibilityRole="button"
          onPress={onGoToMealPlan}
          style={({ pressed }) => [styles.ctaButton, pressed ? styles.pressed : null]}
        >
          <Text style={styles.ctaText}>Go to Meal Plan</Text>
          <Text style={styles.ctaChevron}>›</Text>
        </Pressable>
      </View>

      <View style={[styles.tipCard, { width: contentWidth }]}>
        <View pointerEvents="none" style={styles.tipTextureLayer}>
          <View style={[styles.tipTextureLine, styles.tipTextureLineOne]} />
          <View style={[styles.tipTextureLine, styles.tipTextureLineTwo]} />
          <View style={[styles.tipTextureLine, styles.tipTextureLineThree]} />
          <View style={[styles.tipTextureLine, styles.tipTextureLineFour]} />
        </View>
        <View style={styles.tipHeaderRow}>
          <Text
            adjustsFontSizeToFit
            minimumFontScale={0.86}
            numberOfLines={1}
            style={styles.tipTitle}
          >
            Daily Food Tip
          </Text>
          <Pressable
            accessibilityLabel="Refresh daily food tip"
            accessibilityRole="button"
            onPress={() => setTipIndex((index) => (index + 1) % dailyFoodTips.length)}
            style={({ pressed }) => [styles.refreshButton, pressed ? styles.pressed : null]}
          >
            <Text style={styles.refreshIcon}>↻</Text>
          </Pressable>
        </View>
        <View style={styles.tipDivider} />
        <View style={styles.tipContentRow}>
          <View style={styles.tipBadgeSlot}>
            <View style={styles.tipIconCircle}>
              <TipLightbulbLeafIcon size={52} />
            </View>
          </View>
          <View style={styles.tipCopy}>
            <Text style={styles.tipBody}>{currentTip.body}</Text>
          </View>
          <View style={styles.tipImageSlot}>
            <View style={[styles.tipIllustration, { backgroundColor: currentTip.imageTone }]}>
              <View style={styles.tipPlate} />
              <View style={styles.tipBowl} />
              <View style={styles.tipFoodDot} />
              <View style={styles.tipLeafOne} />
              <View style={styles.tipLeafTwo} />
            </View>
          </View>
        </View>
      </View>

      <ResourceCarouselSection
        contentWidth={contentWidth}
        items={weeklyHighlights}
        onSeeAll={() => setHomeSubPage("highlights")}
        title="Highlights of the Week"
      />
      <ResourceCarouselSection
        contentWidth={contentWidth}
        items={familyKidsIdeas}
        onSeeAll={() => setHomeSubPage("familyKids")}
        title="Family & Kids Food Ideas"
      />
      <ResourceCarouselSection
        contentWidth={contentWidth}
        items={healthyHabits}
        onSeeAll={() => setHomeSubPage("healthyHabits")}
        title="Healthy Habits"
      />
    </AppScreen>
  );
}

function HomeSeeAllPage({
  contentWidth,
  items,
  onBack,
  title,
}: {
  contentWidth: number;
  items: HomeResourceItem[];
  onBack: () => void;
  title: string;
}) {
  return (
    <AppScreen contentContainerStyle={styles.screen}>
      <View style={[styles.seeAllHeader, { width: contentWidth }]}>
        <Pressable
          accessibilityRole="button"
          onPress={onBack}
          style={({ pressed }) => [styles.backButton, pressed ? styles.pressed : null]}
        >
          <Text style={styles.backIcon}>‹</Text>
          <Text style={styles.backText}>Back</Text>
        </Pressable>
        <Text style={styles.seeAllTitle}>{title}</Text>
      </View>

      <View style={[styles.resourceList, { width: contentWidth }]}>
        {items.map((item) => (
          <Pressable
            accessibilityRole="button"
            disabled={!item.url}
            key={item.id}
            onPress={() => openExternalUrl(item.url)}
            style={({ pressed }) => [styles.resourceRow, pressed ? styles.pressed : null]}
          >
            <View style={[styles.resourceThumb, { backgroundColor: item.imageTone }]}>
              <View style={styles.resourceThumbPlate} />
              {item.kind === "video" ? <Text style={styles.resourcePlayIcon}>▶</Text> : null}
            </View>
            <View style={styles.resourceRowText}>
              <Text numberOfLines={2} style={styles.resourceTitle}>
                {item.title}
              </Text>
              <Text numberOfLines={2} style={styles.resourceDescription}>
                {item.description}
              </Text>
              <Text style={styles.resourceType}>{item.typeLabel}</Text>
            </View>
          </Pressable>
        ))}
      </View>
    </AppScreen>
  );
}

function openExternalUrl(url?: string) {
  if (!url) {
    return;
  }
  void Linking.openURL(url).catch(() => undefined);
}

const styles = StyleSheet.create({
  animationCard: {
    alignItems: "center",
    backgroundColor: "#EAF5DF",
    borderRadius: 28,
    justifyContent: "center",
    overflow: "hidden",
  },
  backButton: {
    alignItems: "center",
    alignSelf: "flex-start",
    flexDirection: "row",
    gap: 4,
    minHeight: 36,
  },
  backIcon: {
    color: "#74B72E",
    fontSize: 26,
    fontWeight: "800",
    lineHeight: 30,
  },
  backText: {
    color: "#74B72E",
    fontSize: 16,
    fontWeight: "800",
  },
  ctaButton: {
    alignItems: "center",
    backgroundColor: "#74B72E",
    borderRadius: 18,
    flexDirection: "row",
    height: 52,
    justifyContent: "center",
    marginTop: 12,
  },
  ctaChevron: {
    color: "#FFFFFF",
    fontSize: 21,
    fontWeight: "800",
    marginLeft: 5,
  },
  ctaText: {
    color: "#FFFFFF",
    fontSize: 16,
    fontWeight: "700",
  },
  greeting: {
    color: "#1F2933",
    fontSize: 34,
    fontWeight: "800",
    lineHeight: 38,
  },
  hero: {
    alignItems: "center",
    flexDirection: "row",
    gap: 10,
    minHeight: 150,
  },
  heroCopy: {
    flex: 1,
    justifyContent: "center",
    minWidth: 0,
  },
  heroLottie: {
    height: "112%",
    width: "112%",
  },
  householdBody: {
    color: "#1F2933",
    fontSize: 14.5,
    lineHeight: 19,
    marginTop: 4,
  },
  householdCard: {
    backgroundColor: "#FFFFFF",
    borderColor: "#E5E7EB",
    borderRadius: 24,
    borderWidth: 1,
    elevation: 2,
    marginTop: 10,
    padding: 17,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 7 },
    shadowOpacity: 0.08,
    shadowRadius: 14,
  },
  householdCopy: {
    flex: 1,
  },
  householdIcon: {
    color: "#74B72E",
    fontSize: 24,
    fontWeight: "900",
    lineHeight: 28,
  },
  householdIconCircle: {
    alignItems: "center",
    backgroundColor: "#EAF5DF",
    borderRadius: 22,
    height: 44,
    justifyContent: "center",
    width: 44,
  },
  householdMeta: {
    color: "#6B7280",
    fontSize: 14.5,
    marginTop: 2,
  },
  householdTitle: {
    color: "#1F2933",
    fontSize: 17,
    fontWeight: "700",
  },
  householdTopRow: {
    flexDirection: "row",
    gap: 12,
  },
  pressed: {
    opacity: 0.82,
  },
  profileName: {
    color: "#74B72E",
    fontSize: 43,
    fontWeight: "900",
    lineHeight: 46,
  },
  readyText: {
    color: "#7A8491",
    fontSize: 20,
    fontWeight: "500",
    lineHeight: 24,
    marginTop: 1,
  },
  refreshButton: {
    alignItems: "center",
    backgroundColor: "rgba(255,255,255,0.72)",
    borderColor: "rgba(116,183,46,0.22)",
    borderWidth: 1,
    borderRadius: 18,
    height: 36,
    justifyContent: "center",
    width: 36,
  },
  refreshIcon: {
    color: "#4F8F1F",
    fontSize: 20,
    fontWeight: "800",
    lineHeight: 22,
  },
  resourceDescription: {
    color: "#6B7280",
    fontSize: 14,
    lineHeight: 18,
    marginTop: 5,
  },
  resourceList: {
    gap: 12,
  },
  resourcePlayIcon: {
    color: "#FFFFFF",
    fontSize: 20,
    fontWeight: "900",
    position: "absolute",
  },
  resourceRow: {
    backgroundColor: "#FFFFFF",
    borderColor: "#E5E7EB",
    borderRadius: 20,
    borderWidth: 1,
    elevation: 2,
    flexDirection: "row",
    minHeight: 116,
    padding: 12,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 6 },
    shadowOpacity: 0.07,
    shadowRadius: 12,
  },
  resourceRowText: {
    flex: 1,
    justifyContent: "center",
    marginLeft: 13,
  },
  resourceThumb: {
    alignItems: "center",
    borderRadius: 16,
    height: 92,
    justifyContent: "center",
    overflow: "hidden",
    width: 112,
  },
  resourceThumbPlate: {
    backgroundColor: "rgba(255,255,255,0.72)",
    borderRadius: 30,
    height: 60,
    width: 60,
  },
  resourceTitle: {
    color: "#1F2933",
    fontSize: 16,
    fontWeight: "800",
    lineHeight: 20,
  },
  resourceType: {
    color: "#74B72E",
    fontSize: 13,
    fontWeight: "700",
    marginTop: 6,
  },
  screen: {
    backgroundColor: "#FAFAF6",
    gap: 0,
    paddingHorizontal: 20,
  },
  seeAllHeader: {
    marginBottom: 14,
  },
  seeAllTitle: {
    color: "#1F2933",
    fontSize: 27,
    fontWeight: "800",
    lineHeight: 33,
    marginTop: 6,
  },
  tipBody: {
    color: "#1F2933",
    fontSize: 16.5,
    lineHeight: 21.5,
  },
  tipCard: {
    alignItems: "stretch",
    backgroundColor: "#F1F8E9",
    borderColor: "rgba(116,183,46,0.18)",
    borderRadius: 26,
    borderWidth: 1,
    elevation: 2,
    marginTop: 20,
    minHeight: 198,
    overflow: "hidden",
    paddingHorizontal: 18,
    paddingVertical: 17,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 7 },
    shadowOpacity: 0.08,
    shadowRadius: 14,
  },
  tipBadgeSlot: {
    alignItems: "flex-start",
    justifyContent: "center",
    width: 56,
    zIndex: 1,
  },
  tipCopy: {
    flex: 1,
    justifyContent: "center",
    minWidth: 0,
    zIndex: 1,
  },
  tipContentRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 6,
    minHeight: 100,
    zIndex: 1,
  },
  tipDivider: {
    backgroundColor: "rgba(116,183,46,0.18)",
    height: 1,
    marginBottom: 13,
    marginTop: 10,
    zIndex: 1,
  },
  tipHeaderRow: {
    alignItems: "center",
    flexDirection: "row",
    justifyContent: "space-between",
    minHeight: 36,
    zIndex: 1,
  },
  tipIconCircle: {
    alignItems: "center",
    backgroundColor: "#DFF2CF",
    borderColor: "rgba(116,183,46,0.22)",
    borderRadius: 28,
    borderWidth: 1,
    height: 56,
    justifyContent: "center",
    overflow: "hidden",
    width: 56,
    zIndex: 1,
  },
  tipImageSlot: {
    alignItems: "flex-end",
    justifyContent: "center",
    width: 80,
    zIndex: 1,
  },
  tipIllustration: {
    alignItems: "center",
    borderRadius: 16,
    height: 80,
    justifyContent: "center",
    overflow: "hidden",
    width: 80,
  },
  tipTextureLayer: {
    backgroundColor: "rgba(255, 249, 226, 0.22)",
    bottom: 0,
    left: 0,
    opacity: 0.72,
    position: "absolute",
    right: 0,
    top: 0,
  },
  tipTextureLine: {
    backgroundColor: "rgba(116, 183, 46, 0.08)",
    height: 1,
    position: "absolute",
  },
  tipTextureLineFour: {
    bottom: 27,
    left: 88,
    width: 170,
  },
  tipTextureLineOne: {
    left: 18,
    top: 24,
    width: 130,
  },
  tipTextureLineThree: {
    right: 24,
    top: 104,
    width: 96,
  },
  tipTextureLineTwo: {
    left: 42,
    top: 68,
    width: 210,
  },
  tipLeafOne: {
    backgroundColor: "#7CCB39",
    borderBottomLeftRadius: 12,
    borderTopRightRadius: 12,
    height: 21,
    position: "absolute",
    right: 17,
    top: 22,
    transform: [{ rotate: "-20deg" }],
    width: 32,
  },
  tipLeafTwo: {
    backgroundColor: "#A8D66D",
    borderBottomLeftRadius: 10,
    borderTopRightRadius: 10,
    bottom: 19,
    height: 18,
    left: 20,
    position: "absolute",
    transform: [{ rotate: "18deg" }],
    width: 27,
  },
  tipPlate: {
    backgroundColor: "rgba(255,255,255,0.92)",
    borderRadius: 35,
    height: 50,
    transform: [{ rotate: "-10deg" }],
    width: 60,
  },
  tipBowl: {
    backgroundColor: "#FFFFFF",
    borderBottomLeftRadius: 28,
    borderBottomRightRadius: 28,
    borderTopLeftRadius: 10,
    borderTopRightRadius: 10,
    bottom: 22,
    height: 20,
    position: "absolute",
    width: 52,
  },
  tipFoodDot: {
    backgroundColor: "#F7C948",
    borderRadius: 8,
    height: 15,
    position: "absolute",
    right: 18,
    top: 31,
    width: 15,
  },
  tipTitle: {
    color: "#1F2933",
    flex: 1,
    fontSize: 23,
    fontWeight: "800",
    includeFontPadding: false,
    lineHeight: 28,
    marginRight: 12,
  },
});
