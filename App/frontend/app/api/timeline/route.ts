import { NextResponse } from "next/server";
import { getFilteredTimeline } from "@/lib/services/timeline";

export async function GET() {
    try {
        const timeline = await getFilteredTimeline();
        return NextResponse.json(timeline);
    } catch (error) {
        console.error("Failed to fetch timeline:", error);
        return NextResponse.json(
            { error: "Failed to fetch timeline" },
            { status: 500 }
        );
    }
}
