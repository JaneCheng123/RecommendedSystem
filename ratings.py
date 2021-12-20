"""
A ratings table to be used in a recommender system for online shopping.
csc343, Fall 2021
University of Toronto.

--------------------------------------------------------------------------------
This file is Copyright (c) 2021 Diane Horton and Emily Franklin.
All forms of distribution, whether as given or with any changes, are
expressly prohibited.
--------------------------------------------------------------------------------
"""

from typing import List, Optional


class RatingsTable:
    """A table containing the ratings of at most max_raters different people on
     at most max_items different items. People and items and referred to by
     unique int IDs. Where a rating does not exist from a person for an item,
     the value None is stored.

    === Attributes ===
    max_raters, max_items: Dimensions of this ratings table.
    num_raters, num_items: Current number of different raters and items
        represented by this ratings table.

    === Private Attributes ===
    _table: Ratings for num_raters different people of num_items different
        items. _table[p][i] is the rating by person p of item i.
    _raterIDs: the IDs of the raters. _raterIDs[i] is the ID of the person whose
        ratings are stored in _table[i].
    _itemIDs: the IDs of the items that are rated. _itemIDs[i] is the ID of the
        item whose ratings are stored in table[p][i], for 0 <= p < max_raters.

    === Representation Invariants ===
    0 <= num_raters <= max_raters
    0 <= num_items <= max_items
    for 0 <= p < max_raters and 0 <= i < max_items,
        _table[p][i] is None or 0 <= _table[p][i] <= 10
    len(_table) = len(_raterIDs)
    len(_table[i]) = len(itemIDs) for 0 <= i < max_items

    === Sample Usage ===
    >>>
    """
    _table: List[List[Optional[int]]]
    _raterIDs: List[Optional[int]]
    _itemIDs: List[Optional[int]]
    max_raters:  int
    max_items: int
    num_raters: int
    num_items: int

    def __init__(self, max_raters: int, max_items: int) -> None:
        """Initialize this RatingsTable with max_raters, max_items, but no
        ratings yet.

        >>> rt = RatingsTable(5, 8)
        >>> rt.num_raters
        0
        >>> rt.num_items
        0
        """
        self.max_raters = max_raters
        self.max_items = max_items
        self._table = []
        for _ in range(max_raters):
            self._table.append([None] * max_items)
        self._raterIDs = [None] * max_raters
        self._itemIDs = [None] * max_items
        self.num_raters, self.num_items = 0, 0

    def set_rating(self, who: int, what: int, r: int) -> True:
        """Set the rating of rater who on item what to r.

        If who and/or what doesn't exist in the ratings table, add it first,
        as long as there is room. Return True if successful, and False if who
        and/or what does not exist and there wasn't room to add it.

        >>> # A ratings table with room for ratings of one person on 2 items.
        >>> rt = RatingsTable(1, 2)
        >>> rt.set_rating(123, 55, 3)
        True
        >>> rt.set_rating(123, 66, 4)
        True
        >>> # We can't insert a third rating from this person -- no room!
        >>> rt.set_rating(123, 77, 1)
        False
        >>> # We can't insert a rating from a second person -- no room!
        >>> rt.set_rating(456, 88, 5)
        False
        """
        # Do we already know about who and what?
        if who not in self._raterIDs:
            if self.num_raters < self.max_raters:
                self._raterIDs[self.num_raters] = who
                p = self.num_raters
                self.num_raters += 1
            else:
                return False
        else:
            p = self._raterIDs.index(who)

        if what not in self._itemIDs:
            if self.num_items < self.max_items:
                self._itemIDs[self.num_items] = what
                i = self.num_items
                self.num_items += 1
            else:
                return False
        else:
            i = self._itemIDs.index(what)
        # If we got this far, we have a place to put the rating.
        self._table[p][i] = r
        return True

    def get_rating(self, who: int, what: int) -> Optional[int]:
        """Return the rating of person who for item what, or None if they have
        not rated this item.

        If who or what do not exist in this RatingsTable, return None.

        >>> t = RatingsTable(5, 10)
        >>> t.get_rating(13, 152) is None  # No one has rating anything yet!
        True
        >>> t.set_rating(13, 152, 10)  # Person 13 rates item 152 as 10.
        True
        >>> t.get_rating(13, 152)  # We can look up that rating.
        10
        >>> t.get_rating(13, 159) is None  # They have not rated item 159.
        True
        >>> t.get_rating(16, 152) is None  # Person 16 has not rated item 152.
        True
        """
        if who in self._raterIDs and what in self._itemIDs:
            p = self._raterIDs.index(who)
            i = self._itemIDs.index(what)
            # If there is no rating, the table will still have a None in this
            # position, and it will be returned.
            return self._table[p][i]
        else:
            return None

    def get_all_ratings(self, who: int) -> Optional[List[Optional[int]]]:
        """Return all ratings by who in this RatingsTable, or None if
        who is not in this RatingsTable.

        >>> t = RatingsTable(4, 5)  # There can be 4 raters and 5 items.
        >>> t.get_all_ratings(13) is None  # No one has rating anything yet!
        True
        >>> t.set_rating(13, 152, 10)  # Person 13 rates item 152 as 10.
        True
        >>> t.set_rating(13, 268, 7)  # Person 13 rates another item.
        True
        >>> t.set_rating(13, 268, 6)  # They change their rating for item 268.
        True
        >>> t.get_all_ratings(13)
        [10, 6, None, None, None]
        >>> t.get_all_ratings(14) is None  # Person 14 has rating nothing yet.
        True
        """
        if who in self._raterIDs:
            p = self._raterIDs.index(who)
            return self._table[p]
        else:
            return None


if __name__ == '__main__':
    import doctest
    doctest.testmod()
