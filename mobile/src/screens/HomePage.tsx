import { useMemo, useState } from "react";
import {
  Image,
  Linking,
  Pressable,
  StyleSheet,
  Text,
  useWindowDimensions,
  View,
} from "react-native";
import LottieView from "lottie-react-native";

import { ResourceCarouselSection } from "../components/home/ResourceCarouselSection";
import { VideoPlayBadge } from "../components/home/VideoPlayBadge";
import { ChevronDownIcon } from "../components/icons/ChevronDownIcon";
import { DailyFoodTipIcon } from "../components/icons/DailyFoodTipIcon";
import { HouseholdIcon } from "../components/icons/HouseholdIcon";
import { AppScreen } from "../components/ui/AppScreen";
import { RotatingRefreshButton } from "../components/ui/RotatingRefreshButton";
import {
  dailyFoodTips,
  familyKidsIdeas,
  healthyHabits,
  weeklyHighlights,
  type HomeResourceItem,
} from "../data/homeContent";

const cookingLottie = require("../../assets/home/welcome/cooking_lottie.json");
const HOME_CARD_ICON_SIZE = 58;
const DAILY_TIP_IMAGE_SLOT_RADIUS = 20;
const DAILY_TIP_IMAGE_SLOT_SIZE = 104;
const DAILY_TIP_REFRESH_BUTTON_SIZE = 38;
const DAILY_TIP_REFRESH_ICON_SIZE = 22;

type HomePageProps = {
  activeProfileName: string;
  householdName: string;
  isSetupComplete: boolean;
  onGoToHousehold: () => void;
  onGoToMealPlan: () => void;
  profileCount: number;
  scrollToTopSignal?: number;
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
  onGoToHousehold,
  onGoToMealPlan,
  profileCount,
  scrollToTopSignal,
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
  const householdCtaLabel = hasProfiles ? "Go to Meal Plan" : "Add Member Profile";
  const householdCtaTarget = hasProfiles ? onGoToMealPlan : onGoToHousehold;

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
        scrollToTopSignal={scrollToTopSignal}
        title={section.title}
      />
    );
  }

  return (
    <AppScreen contentContainerStyle={styles.screen} scrollToTopSignal={scrollToTopSignal}>
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
          <View pointerEvents="none" style={styles.animationBackdrop} />
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
          <HouseholdIcon height={HOME_CARD_ICON_SIZE} width={HOME_CARD_ICON_SIZE} />
          <View style={styles.householdCopy}>
            <Text numberOfLines={1} style={styles.householdTitle}>
              {isSetupComplete ? safeHouseholdName : "Set up your household"}
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
          onPress={householdCtaTarget}
          style={({ pressed }) => [styles.ctaButton, pressed ? styles.pressed : null]}
        >
          <Text style={styles.ctaText}>{householdCtaLabel}</Text>
          <View style={styles.ctaChevronIcon}>
            <ChevronDownIcon color="#FFFFFF" size={16} />
          </View>
        </Pressable>
      </View>

      <View style={[styles.tipCard, { width: contentWidth }]}>
        <View style={styles.tipHeaderRow}>
          <DailyFoodTipIcon height={HOME_CARD_ICON_SIZE} width={HOME_CARD_ICON_SIZE} />
          <Text
            adjustsFontSizeToFit
            minimumFontScale={0.86}
            numberOfLines={1}
            style={styles.tipTitle}
          >
            Daily Food Tip
          </Text>
          <RotatingRefreshButton
            accessibilityLabel="Refresh daily food tip"
            iconSize={DAILY_TIP_REFRESH_ICON_SIZE}
            onPress={() => setTipIndex((index) => (index + 1) % dailyFoodTips.length)}
            size={DAILY_TIP_REFRESH_BUTTON_SIZE}
          />
        </View>
        <View style={styles.tipBodyRow}>
          <Text style={styles.tipBody}>{currentTip.body}</Text>
          <View style={[styles.tipImagePlaceholder, { backgroundColor: currentTip.imageTone }]}>
            <Image
              resizeMode="contain"
              source={currentTip.image}
              style={styles.tipImage}
            />
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
  scrollToTopSignal,
  title,
}: {
  contentWidth: number;
  items: HomeResourceItem[];
  onBack: () => void;
  scrollToTopSignal?: number;
  title: string;
}) {
  return (
    <AppScreen contentContainerStyle={styles.screen} scrollToTopSignal={scrollToTopSignal}>
      <View style={[styles.seeAllHeader, { width: contentWidth }]}>
        <Pressable
          accessibilityRole="button"
          onPress={onBack}
          style={({ pressed }) => [styles.backButton, pressed ? styles.pressed : null]}
        >
          <View style={styles.backChevronIcon}>
            <ChevronDownIcon color="#74B72E" size={17} />
          </View>
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
              {item.imageAsset ? (
                <Image resizeMode="cover" source={item.imageAsset} style={styles.resourceThumbImage} />
              ) : (
                <View style={styles.resourceThumbPlate} />
              )}
              {item.kind === "video" ? <VideoPlayBadge /> : null}
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
    borderRadius: 28,
    justifyContent: "center",
    overflow: "hidden",
    position: "relative",
  },
  animationBackdrop: {
    backgroundColor: "#EAF5DF",
    borderRadius: 28,
    bottom: 0,
    left: 0,
    position: "absolute",
    right: 0,
    top: 20,
  },
  backButton: {
    alignItems: "center",
    alignSelf: "flex-start",
    flexDirection: "row",
    gap: 4,
    minHeight: 36,
  },
  backChevronIcon: {
    transform: [{ rotate: "90deg" }],
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
  ctaChevronIcon: {
    marginLeft: 5,
    transform: [{ rotate: "-90deg" }],
  },
  ctaText: {
    color: "#FFFFFF",
    fontSize: 16,
    fontWeight: "700",
    includeFontPadding: false,
    lineHeight: 20,
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
    alignItems: "center",
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
  resourceDescription: {
    color: "#6B7280",
    fontSize: 14,
    lineHeight: 18,
    marginTop: 5,
  },
  resourceList: {
    gap: 12,
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
  resourceThumbImage: {
    height: "100%",
    width: "100%",
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
    flex: 1,
    fontSize: 17.8,
    lineHeight: 24.8,
    minWidth: 0,
  },
  tipBodyRow: {
    alignItems: "flex-start",
    flexDirection: "row",
    gap: 14,
    marginTop: 14,
    zIndex: 1,
  },
  tipCard: {
    alignItems: "stretch",
    backgroundColor: "#F1F8E9",
    borderColor: "rgba(116,183,46,0.18)",
    borderRadius: 26,
    borderWidth: 1,
    elevation: 2,
    marginTop: 20,
    minHeight: 216,
    overflow: "hidden",
    paddingHorizontal: 20,
    paddingVertical: 20,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 7 },
    shadowOpacity: 0.08,
    shadowRadius: 14,
  },
  tipHeaderRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
    minHeight: 58,
    zIndex: 1,
  },
  tipImagePlaceholder: {
    alignItems: "center",
    borderColor: "rgba(255,255,255,0.56)",
    borderRadius: DAILY_TIP_IMAGE_SLOT_RADIUS,
    borderWidth: 1,
    height: DAILY_TIP_IMAGE_SLOT_SIZE,
    justifyContent: "center",
    overflow: "hidden",
    width: DAILY_TIP_IMAGE_SLOT_SIZE,
  },
  tipImage: {
    height: "100%",
    width: "100%",
  },
  tipTitle: {
    color: "#1F2933",
    flex: 1,
    fontSize: 24,
    fontWeight: "800",
    includeFontPadding: false,
    lineHeight: 30,
  },
});
