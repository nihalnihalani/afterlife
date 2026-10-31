// SYNTHETIC fact sheets, written at the event for the demo. None of these places are real.
export type FactSheet = { id: string; title: string; facts: string[] }

export const FACT_SHEETS: FactSheet[] = [
    {
        id: "t1",
        title: "Hollowbrook Manor",
        facts: [
            "Built 1887 by a lantern maker named Elias Hollowbrook",
            "The brass lantern in the entrance hall relights itself at 3:12 a.m.",
            "Ground floor: ballroom with a cracked mirror, library missing one volume",
            "The cellar door is warm to the touch in winter",
            "Upper floor: nursery with a rocking horse that faces the window",
            "Investigators recorded a humming at 40 Hz near the stairwell",
            "Caretaker Mrs. Abernathy reports footsteps counting to seven",
            "The missing library volume was found inside the cellar wall in 1952",
            "Temperature drops 6 degrees in the nursery when the lantern relights",
            "Local legend: Elias still trims the wicks every night",
        ],
    },
    {
        id: "t2",
        title: "St. Vesper Asylum",
        facts: [
            "Opened 1903, closed 1971 after a ward flooded",
            "Ward C has a clock frozen at 4:40",
            "Ground floor: admissions desk with a ledger signed in green ink",
            "A wheelchair in the east corridor is always found facing north",
            "Upper floor: chapel with one stained-glass panel replaced by plain glass",
            "Night guard Tomas Reyes heard a piano with no keys pressed",
            "Water stains on the Ward C ceiling form the shape of a door",
            "The green-ink ledger's last entry is dated a week after closure",
            "EVP session captured the word 'admit' three times",
            "Visitors smell lilies in the chapel though none grow nearby",
        ],
    },
    {
        id: "t3",
        title: "Gallows Lane Inn",
        facts: [
            "Coaching inn from 1790 on the old toll road",
            "Room 9 has no window but a draft from the north wall",
            "Ground floor: taproom with a bell that rings when no one pulls it",
            "Stable yard cobbles are warm after midnight",
            "Upper floor: a corridor that guests say is longer at night",
            "Innkeeper Maud Pell kept a guest book with one name crossed out 40 times",
            "Guests report a knock pattern: three, pause, one",
            "A coachman's lantern hangs in Room 9 though no one hung it",
            "Investigators measured the night corridor: 2 meters longer by laser",
            "The crossed-out name belongs to a toll collector who vanished in 1803",
        ],
    },
]

export const SECTIONS = ["Arrival", "Ground floor", "Upper floors", "The anomaly", "Witness accounts", "Conclusion"]
